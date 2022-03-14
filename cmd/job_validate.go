package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	validateTimeout = time.Minute * 5
)

func jobValidateCommand(l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository, projectName, host string) *cli.Command {
	var (
		verbose       bool
		namespaceName string
	)
	cmd := &cli.Command{
		Use:     "validate",
		Short:   "Run basic checks on all jobs",
		Long:    "Check if specifications are valid for deployment",
		Example: "optimus job validate",
		RunE: func(c *cli.Command, args []string) error {
			namespace := conf.Namespaces[namespaceName]
			if namespace == nil {
				return fmt.Errorf("namespace [%s] is not found", namespaceName)
			}
			jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
			jobSpecRepo := local.NewJobSpecRepository(
				jobSpecFs,
				local.NewJobSpecAdapter(pluginRepo),
			)
			l.Info(fmt.Sprintf("Validating job specifications for project: %s, namespace: %s", projectName, namespace.Name))
			start := time.Now()
			jobSpecs, err := jobSpecRepo.GetAll()
			if err != nil {
				return fmt.Errorf("directory '%s': %v", namespace.Job.Path, err)
			}

			if err := validateJobSpecificationRequest(l, projectName, namespace.Name, pluginRepo, jobSpecs, host, verbose); err != nil {
				return err
			}
			l.Info(coloredSuccess("Jobs validated successfully, took %s", time.Since(start).Round(time.Second)))
			return nil
		},
	}
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "targetted namespace for validating job")
	cmd.MarkFlagRequired("namespace")
	return cmd
}

func validateJobSpecificationRequest(l log.Logger, projectName string, namespace string,
	pluginRepo models.PluginRepository, jobSpecs []models.JobSpec, host string, verbose bool) (err error) {
	adapt := v1handler.NewAdapter(pluginRepo, models.DatastoreRegistry)

	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	dumpTimeoutCtx, dumpCancel := context.WithTimeout(context.Background(), validateTimeout)
	defer dumpCancel()

	adaptedJobSpecs := []*pb.JobSpecification{}
	for _, spec := range jobSpecs {
		adaptJob, err := adapt.ToJobProto(spec)
		if err != nil {
			return fmt.Errorf("failed to serialize: %s: %w", spec.Name, err)
		}
		adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
	}

	runtime := pb.NewRuntimeServiceClient(conn)
	respStream, err := runtime.CheckJobSpecifications(dumpTimeoutCtx, &pb.CheckJobSpecificationsRequest{
		ProjectName:   projectName,
		Jobs:          adaptedJobSpecs,
		NamespaceName: namespace,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Validate process took too long, timing out"))
		}
		return fmt.Errorf("validate request failed: %w", err)
	}

	ackCounter := 0
	failedCounter := 0
	totalJobs := len(jobSpecs)

	spinner := NewProgressBar()
	if !verbose {
		spinner.StartProgress(totalJobs, "validating jobs")
	}

	var validateErrors []string
	var streamError error
	for {
		resp, err := respStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			streamError = err
			break
		}
		if resp.Ack {
			// ack for the job spec
			if !resp.GetSuccess() {
				failedCounter++
				validateErrors = append(validateErrors, fmt.Sprintf("failed to validate: %s, %s\n", resp.GetJobName(), resp.GetMessage()))
			}
			ackCounter++
			if verbose {
				l.Info(fmt.Sprintf("%d/%d. %s successfully checked", ackCounter, totalJobs, resp.GetJobName()))
			}
			spinner.SetProgress(ackCounter)
		} else {
			if verbose {
				// ordinary progress event
				l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
			}
		}
	}
	spinner.Stop()

	if len(validateErrors) > 0 {
		if verbose {
			for i, reqErr := range validateErrors {
				l.Error(fmt.Sprintf("%d. %s", i+1, reqErr))
			}
		}
	} else if streamError != nil && ackCounter == totalJobs && failedCounter == 0 {
		// if we have uploaded all jobs successfully, further steps in pipeline
		// should not cause errors to fail and should end with warnings if any.
		l.Warn(coloredNotice("request ended with warning"), "err", streamError)
		return nil
	}
	return streamError
}
