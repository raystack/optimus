package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/odpf/optimus/config"

	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	validateTimeout = time.Minute * 5
)

func validateCommand(l log.Logger, host string, pluginRepo models.PluginRepository, jobSpecRepo JobSpecRepository,
	conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "validate",
		Short: "check if specifications are valid for deployment",
	}
	if jobSpecRepo != nil {
		cmd.AddCommand(validateJobCommand(l, host, pluginRepo, jobSpecRepo, conf))
	}
	return cmd
}

func validateJobCommand(l log.Logger, host string, pluginRepo models.PluginRepository, jobSpecRepo JobSpecRepository,
	conf config.Provider) *cli.Command {
	var projectName string
	var namespace string
	cmd := &cli.Command{
		Use:     "job",
		Short:   "run basic checks on all jobs",
		Example: "optimus validate job",
	}

	cmd.Flags().StringVar(&projectName, "project", conf.GetProject().Name, "name of the project")
	cmd.Flags().StringVar(&namespace, "namespace", conf.GetNamespace().Name, "namespace")
	cmd.RunE = func(c *cli.Command, args []string) error {
		start := time.Now()
		jobSpecs, err := jobSpecRepo.GetAll()
		if err != nil {
			return err
		}

		if err := validateJobSpecificationRequest(l, projectName, namespace, pluginRepo, jobSpecs, host); err != nil {
			return err
		}
		l.Info("jobs successfully validated")
		l.Info(fmt.Sprintf("validated in %s", time.Since(start).String()))

		return nil
	}
	return cmd
}

func validateJobSpecificationRequest(l log.Logger, projectName string, namespace string,
	pluginRepo models.PluginRepository, jobSpecs []models.JobSpec, host string) (err error) {
	adapt := v1handler.NewAdapter(pluginRepo, models.DatastoreRegistry)

	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service")
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
			return errors.Wrapf(err, "failed to serialize: %s", spec.Name)
		}
		adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
	}

	l.Info("validating please wait...")

	runtime := pb.NewRuntimeServiceClient(conn)
	respStream, err := runtime.CheckJobSpecifications(dumpTimeoutCtx, &pb.CheckJobSpecificationsRequest{
		ProjectName:   projectName,
		Jobs:          adaptedJobSpecs,
		NamespaceName: namespace,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("validate process took too long, timing out")
		}
		return errors.Wrapf(err, "validate request failed")
	}

	ackCounter := 0
	failedCounter := 0
	totalJobs := len(jobSpecs)
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
				validateErrors = append(validateErrors, fmt.Sprintf("unable to check: %s, %s\n", resp.GetJobName(), resp.GetMessage()))
			}
			ackCounter++
			l.Info(fmt.Sprintf("%d/%d. %s successfully checked", ackCounter, totalJobs, resp.GetJobName()))
		} else {
			// ordinary progress event
			l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
		}
	}
	if len(validateErrors) > 0 {
		for i, reqErr := range validateErrors {
			l.Error(fmt.Sprintf("%d. %s", i+1, reqErr))
		}
	} else if streamError != nil && ackCounter == totalJobs && failedCounter == 0 {
		// if we have uploaded all jobs successfully, further steps in pipeline
		// should not cause errors to fail and should end with warnings if any.
		l.Warn(coloredNotice("requested ended with warning"), "err", streamError)
		return nil
	}
	return streamError
}
