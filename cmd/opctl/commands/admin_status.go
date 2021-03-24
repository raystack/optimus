package commands

import (
	"context"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	pb "github.com/odpf/optimus/api/proto/v1"
)

const (
	adminStatusTimeout = time.Second * 10
)

func adminGetStatusCommand(l logger) *cli.Command {
	var (
		optimusHost string
		projectName string
	)
	cmd := &cli.Command{
		Use:     "status",
		Short:   "Get current job status",
		Example: `opctl admin get status sample_replace --project \"project-id\" --scheduled-at \"2020-01-02T15:04:05"\" `,
		Args:    cli.MinimumNArgs(1),
	}
	cmd.Flags().StringVar(&projectName, "project", "", "name of the tenant")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&optimusHost, "host", "", "optimus service endpoint url")
	cmd.MarkFlagRequired("host")

	cmd.Run = func(c *cli.Command, args []string) {
		jobName := args[0]
		l.Printf("requesting status for project %s, job %s[%s] at %s\nplease wait...\n",
			projectName, jobName, scheduledAt, optimusHost)

		if err := getJobStatusRequest(l, jobName, scheduledAt, optimusHost, projectName); err != nil {
			l.Print(err)
			l.Print(errRequestFail)
			os.Exit(1)
		}
	}
	return cmd
}

func getJobStatusRequest(l logger, jobName, scheduledAt, host, projectName string) error {
	var err error
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println("can't reach optimus service, timing out")
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

	jobStatuses := jobStatusResponse.GetAll()

	sort.Slice(jobStatuses, func(i, j int) bool {
		return jobStatuses[i].ScheduledAt.Seconds < jobStatuses[j].ScheduledAt.Seconds
	})

	for _, status := range jobStatuses {
		l.Printf("%s - %s\n", status.GetScheduledAt().AsTime(), status.GetState())
	}
	return err
}
