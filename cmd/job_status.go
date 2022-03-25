package cmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	jobStatusTimeout = time.Second * 30
)

func jobStatusCommand(l log.Logger, defaultProjectName, defaultHost string) *cli.Command {
	cmd := &cli.Command{
		Use:     "status",
		Short:   "Get current job status",
		Example: `optimus job status <sample_job_goes_here> [--project \"project-id\"]`,
		Args:    cli.MinimumNArgs(1),
	}
	projectName := defaultProjectName
	host := defaultHost
	cmd.Flags().StringVarP(&projectName, "project", "p", projectName, "Project name of optimus managed repository")
	cmd.Flags().StringVar(&host, "host", defaultHost, "Optimus service endpoint url")

	cmd.RunE = func(c *cli.Command, args []string) error {
		jobName := args[0]
		l.Info(fmt.Sprintf("Requesting status for project %s, job %s from %s",
			projectName, jobName, host))

		return getJobStatusRequest(l, jobName, host, projectName)
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

	timeoutCtx, cancel := context.WithTimeout(context.Background(), jobStatusTimeout)
	defer cancel()

	jobRun := pb.NewJobRunServiceClient(conn)
	spinner := NewProgressBar()
	spinner.Start("please wait...")
	jobStatusResponse, err := jobRun.JobStatus(timeoutCtx, &pb.JobStatusRequest{
		ProjectName: projectName,
		JobName:     jobName,
	})
	spinner.Stop()
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}

	jobStatuses := jobStatusResponse.GetStatuses()
	sort.Slice(jobStatuses, func(i, j int) bool {
		return jobStatuses[i].ScheduledAt.Seconds < jobStatuses[j].ScheduledAt.Seconds
	})

	for _, status := range jobStatuses {
		l.Info(fmt.Sprintf("%s - %s", status.GetScheduledAt().AsTime(), status.GetState()))
	}
	l.Info(coloredSuccess("\nFound %d run instances.", len(jobStatuses)))
	return err
}
