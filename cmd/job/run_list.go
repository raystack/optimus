package job

import (
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
)

const jobStatusTimeout = time.Second * 30

type runListCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	startDate string
	endDate   string
}

// NewRunListCommand initializes run list command
func NewRunListCommand(clientConfig *config.ClientConfig) *cobra.Command {
	run := &runListCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "list-runs",
		Short:   "Get Job run details",
		Example: `optimus job runs <sample_job_goes_here> [--project \"project-id\"] [--start_date \"2006-01-02T15:04:05Z07:00\" --end_date \"2006-01-02T15:04:05Z07:00\"]`,
		Args:    cobra.MinimumNArgs(1),
		RunE:    run.RunE,
		PreRunE: run.PreRunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	cmd.Flags().String("host", defaultHost, "Optimus service endpoint url")
	cmd.Flags().StringVar(&run.startDate, "start_date", "", "start date of job run")
	cmd.Flags().StringVar(&run.endDate, "end_date", "", "end date of job run")
	return cmd
}

func (r *runListCommand) PreRunE(cmd *cobra.Command, args []string) error {
	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	return nil
}

func (r *runListCommand) RunE(cmd *cobra.Command, args []string) error {
	jobName := args[0]
	r.logger.Info(fmt.Sprintf("Requesting status for project %s, job %s from %s",
		r.clientConfig.Project.Name, jobName, r.clientConfig.Host))

	if err := r.validateDateArgs(r.startDate, r.endDate); err != nil {
		return err
	}
	var err error
	var req *pb.JobRunRequest
	req, err = r.createJobRunRequest(jobName, r.startDate, r.endDate)
	if err != nil {
		return err
	}
	return r.callJobRun(req)
}

func (r *runListCommand) callJobRun(jobRunRequest *pb.JobRunRequest) error {
	conn, err := connectivity.NewConnectivity(r.clientConfig.Host, jobStatusTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	run := pb.NewJobRunServiceClient(conn.GetConnection())
	jobRunResponse, err := run.JobRun(conn.GetContext(), jobRunRequest)
	spinner.Stop()
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", jobRunRequest.JobName, err)
	}

	jobRuns := jobRunResponse.GetJobRuns()
	for _, jobRun := range jobRuns {
		r.logger.Info(fmt.Sprintf("%s - %s", jobRun.GetScheduledAt().AsTime(), jobRun.GetState()))
	}
	r.logger.Info("\nFound %d jobRun instances.", len(jobRuns))
	return nil
}

func (r *runListCommand) createJobRunRequest(jobName, startDate, endDate string) (*pb.JobRunRequest, error) {
	var req *pb.JobRunRequest
	if startDate == "" && endDate == "" {
		req = &pb.JobRunRequest{
			ProjectName: r.clientConfig.Project.Name,
			JobName:     jobName,
		}
		return req, nil
	}
	start, err := time.Parse(time.RFC3339, startDate)
	if err != nil {
		return req, fmt.Errorf("start_date %w", err)
	}
	sDate := timestamppb.New(start)
	end, err := time.Parse(time.RFC3339, endDate)
	if err != nil {
		return req, fmt.Errorf("end_date %w", err)
	}
	eDate := timestamppb.New(end)
	req = &pb.JobRunRequest{
		ProjectName: r.clientConfig.Project.Name,
		JobName:     jobName,
		StartDate:   sDate,
		EndDate:     eDate,
	}
	return req, nil
}

func (r *runListCommand) validateDateArgs(startDate, endDate string) error {
	if startDate == "" && endDate != "" {
		return errors.New("please provide the start date")
	}
	if startDate != "" && endDate == "" {
		return errors.New("please provide the end date")
	}
	return nil
}
