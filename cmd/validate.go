package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	v1handler "github.com/odpf/optimus/api/handler/v1"

	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/store"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	validateTimeout = time.Minute * 3
)

func validateCommand(l logger, host string, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "validate",
		Short: "check if specifications are valid for deployment",
	}
	if jobSpecRepo != nil {
		cmd.AddCommand(validateJobCommand(l, host, jobSpecRepo))
	}
	return cmd
}

func validateJobCommand(l logger, host string, jobSpecRepo store.JobSpecRepository) *cli.Command {
	var projectName string
	var namespace string
	cmd := &cli.Command{
		Use:     "job",
		Short:   "run basic checks on all jobs",
		Example: "optimus validate job",
	}
	cmd.Flags().StringVar(&projectName, "project", "", "name of the project")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&namespace, "namespace", "", "namespace")
	cmd.MarkFlagRequired("namespace")

	cmd.RunE = func(c *cli.Command, args []string) error {
		start := time.Now()
		jobSpecs, err := jobSpecRepo.GetAll()
		if err != nil {
			return err
		}
		if err := validateJobSpecificationRequest(l, projectName, namespace, jobSpecs, host); err != nil {
			return err
		}
		l.Println("jobs successfully validated")
		l.Printf("validated in %s\n", time.Since(start).String())

		return nil
	}

	return cmd
}

func validateJobSpecificationRequest(l logger, projectName string, namespace string, jobSpecs []models.JobSpec, host string) (err error) {
	adapt := v1handler.NewAdapter(models.TaskRegistry, models.HookRegistry, models.DatastoreRegistry)

	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println("can't reach optimus service")
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

	l.Println("validating please wait...")

	runtime := pb.NewRuntimeServiceClient(conn)
	respStream, err := runtime.CheckJobSpecifications(dumpTimeoutCtx, &pb.CheckJobSpecificationsRequest{
		ProjectName: projectName,
		Jobs:        adaptedJobSpecs,
		Namespace:   namespace,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println("validate process took too long, timing out")
		}
		return errors.Wrapf(err, "validate request failed")
	}

	jobCounter := 0
	totalJobs := len(jobSpecs)
	totalErrors := []string{}
	for {
		resp, err := respStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrapf(err, "failed to receive check ack")
		}
		if resp.Ack {
			// ack for the job spec
			if !resp.GetSuccess() {
				totalErrors = append(totalErrors, fmt.Sprintf("unable to check: %s, %s\n", resp.GetJobName(), resp.GetMessage()))
			}
			jobCounter++
			l.Printf("%d/%d. %s successfully checked\n", jobCounter, totalJobs, resp.GetJobName())
		} else {
			// ordinary progress event
			l.Printf("info '%s': %s\n", resp.GetJobName(), resp.GetMessage())
		}
	}
	if len(totalErrors) > 0 {
		l.Println("errors:")
		for i, reqErr := range totalErrors {
			l.Printf("%d. %s", i, reqErr)
		}
	}
	return nil
}
