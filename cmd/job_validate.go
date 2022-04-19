package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

const (
	validateTimeout = time.Minute * 5
)

func jobValidateCommand(conf *config.ClientConfig) *cli.Command {
	var (
		verbose       bool
		namespaceName string
		projectName   string
		host          string
	)
	cmd := &cli.Command{
		Use:     "validate",
		Short:   "Run basic checks on all jobs",
		Long:    "Check if specifications are valid for deployment",
		Example: "optimus job validate",
		RunE: func(c *cli.Command, args []string) error {
			projectName = conf.Project.Name
			host = conf.Host
			l := initClientLogger(conf.Log)
			pluginRepo := models.PluginRegistry
			namespace, err := conf.GetNamespaceByName(namespaceName)
			if err != nil {
				return err
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
				return fmt.Errorf("directory '%s': %w", namespace.Job.Path, err)
			}

			if err := validateJobSpecificationRequest(l, projectName, namespace.Name, jobSpecs, host, verbose); err != nil {
				return err
			}
			l.Info(coloredSuccess("Jobs validated successfully, took %s", time.Since(start).Round(time.Second)))
			return nil
		},
	}
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "Namespace of the resource within project")
	cmd.MarkFlagRequired("namespace")
	return cmd
}

func validateJobSpecificationRequest(l log.Logger, projectName, namespace string, jobSpecs []models.JobSpec, host string, verbose bool) (err error) {
	ctx, conn, closeConn, err := initClientConnection(host, validateTimeout)
	if err != nil {
		return err
	}
	defer closeConn()

	adaptedJobSpecs := []*pb.JobSpecification{}
	for _, spec := range jobSpecs {
		adaptJob := v1handler.ToJobProto(spec)
		adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
	}

	job := pb.NewJobSpecificationServiceClient(conn)
	respStream, err := job.CheckJobSpecifications(ctx, &pb.CheckJobSpecificationsRequest{
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
			if errors.Is(err, io.EOF) {
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
		} else if verbose {
			// ordinary progress event
			l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
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
