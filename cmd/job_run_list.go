package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

const (
	jobStatusTimeout = time.Second * 30
)

type jobRunCmdArg struct {
	projectName string
	jobName     string
	startDate   string
	endDate     string
}

type getRequestFn func(jobRunCmdArg) (*pb.JobRunRequest, error)

func jobRunListCommand(l log.Logger, defaultProjectName, defaultHost string) *cli.Command {
	cmd := &cli.Command{
		Use:     "runs",
		Short:   "Get current job runs",
		Example: `optimus job runs <sample_job_goes_here> [--project \"project-id\"] [--start_date \"2006-01-02T15:04:05Z07:00\" --end_date \"2006-01-02T15:04:05Z07:00\"]`,
		Args:    cli.MinimumNArgs(1),
	}
	projectName := defaultProjectName
	host := defaultHost
	var startDate string
	var endDate string
	cmd.Flags().StringVarP(&projectName, "project", "p", projectName, "Project name of optimus managed repository")
	cmd.Flags().StringVar(&host, "host", defaultHost, "Optimus service endpoint url")
	cmd.Flags().StringVar(&startDate, "start_date", "", "start date of job run")
	cmd.Flags().StringVar(&endDate, "end_date", "", "end date of job run")
	cmd.RunE = func(c *cli.Command, args []string) error {
		jobName := args[0]
		l.Info(fmt.Sprintf("Requesting status for project %s, job %s from %s",
			projectName, jobName, host))
		arg := jobRunCmdArg{
			projectName: projectName,
			jobName:     jobName,
			startDate:   startDate,
			endDate:     endDate,
		}
		return getJobRunList(l, host, arg)
	}
	return cmd
}

func getJobRunList(l log.Logger, host string, arg jobRunCmdArg) error {
	// get the last schedule run
	if arg.startDate == "" && arg.endDate == "" {
		return callJobRun(l, host, arg, getJobRunRequest)
	}
	// get runs between start_date and end_date (both start_date and end_date are inclusive)
	return callJobRun(l, host, arg, getJobRunRequestWithDate)
}

func callJobRun(l log.Logger, host string, arg jobRunCmdArg, fn getRequestFn) error {
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

	run := pb.NewJobRunServiceClient(conn)
	req, err := fn(arg)
	if err != nil {
		return err
	}

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	jobRunResponse, err := run.JobRun(timeoutCtx, req)
	spinner.Stop()
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", arg.jobName, err)
	}

	jobRuns := jobRunResponse.GetJobRuns()

	for _, jobRun := range jobRuns {
		l.Info(fmt.Sprintf("%s - %s", jobRun.GetScheduledAt().AsTime(), jobRun.GetState()))
	}
	l.Info(coloredSuccess("\nFound %d jobRun instances.", len(jobRuns)))
	return err
}

func getJobRunRequest(arg jobRunCmdArg) (*pb.JobRunRequest, error) {
	req := &pb.JobRunRequest{
		ProjectName: arg.projectName,
		JobName:     arg.jobName,
	}
	return req, nil
}

func getJobRunRequestWithDate(arg jobRunCmdArg) (*pb.JobRunRequest, error) {
	var req *pb.JobRunRequest
	query, err := validateDateRange(arg.startDate, arg.endDate)
	if err != nil {
		return req, fmt.Errorf("request failed for job %s: %w", arg.jobName, err)
	}
	req = &pb.JobRunRequest{
		ProjectName: arg.projectName,
		JobName:     arg.jobName,
		StartDate:   timestamppb.New(query.StartDate),
		EndDate:     timestamppb.New(query.EndDate),
	}
	return req, nil
}

func validateDateRange(startDate, endDate string) (models.JobQuery, error) {
	var jobQuery models.JobQuery
	if startDate == "" && endDate != "" {
		return jobQuery, errors.New("please provide the start date")
	}
	if startDate != "" && endDate == "" {
		return jobQuery, errors.New("please provide the end date")
	}
	sDate, err := time.Parse(time.RFC3339, startDate)
	if err != nil {
		return jobQuery, fmt.Errorf("start_date %w", err)
	}
	jobQuery.StartDate = sDate
	eDate, err := time.Parse(time.RFC3339, endDate)
	if err != nil {
		return jobQuery, fmt.Errorf("end_date %w", err)
	}
	jobQuery.EndDate = eDate
	return jobQuery, nil
}
