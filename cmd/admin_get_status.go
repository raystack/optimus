package cmd

import (
	"context"
	"fmt"
	"sort"
	"time"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	adminStatusTimeout = time.Second * 10
)

func adminGetStatusCommand(l log.Logger) *cli.Command {
	var (
		optimusHost string
		projectName string
	)
	cmd := &cli.Command{
		Use:     "status",
		Short:   "Get current job status",
		Example: `optimus admin get status sample_replace --project \"project-id\"`,
		Args:    cli.MinimumNArgs(1),
	}
	cmd.Flags().StringVar(&projectName, "project", "", "name of the tenant")
	cmd.Flags().StringVar(&optimusHost, "host", "", "optimus service endpoint url")

	cmd.RunE = func(c *cli.Command, args []string) error {
		jobName := args[0]
		l.Info(fmt.Sprintf("requesting status for project %s, job %s at %s\nplease wait...",
			projectName, jobName, optimusHost))

		if err := getJobStatusRequest(l, jobName, optimusHost, projectName); err != nil {
			return err
		}

		return nil
	}
	return cmd
}

func getJobStatusRequest(l log.Logger, jobName, host, projectName string) error {
	var err error
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service, timing out")
		}
		return err
	}
	defer conn.Close()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), adminStatusTimeout)
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	jobStatusResponse, err := runtime.JobStatus(timeoutCtx, &pb.JobStatusRequest{
		ProjectName: projectName,
		JobName:     jobName,
	})
	if err != nil {
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	jobStatuses := jobStatusResponse.GetStatuses()

	sort.Slice(jobStatuses, func(i, j int) bool {
		return jobStatuses[i].ScheduledAt.Seconds < jobStatuses[j].ScheduledAt.Seconds
	})

	for _, status := range jobStatuses {
		l.Info(fmt.Sprintf("%s - %s", status.GetScheduledAt().AsTime(), status.GetState()))
	}
	return err
}
