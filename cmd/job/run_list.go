package job

import (
	"errors"
	"fmt"
	"time"

	saltConfig "github.com/odpf/salt/config"
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
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	startDate   string
	endDate     string
	projectName string
	host        string
}

// NewRunListCommand initializes run list command
func NewRunListCommand() *cobra.Command {
	run := &runListCommand{
		clientConfig: &config.ClientConfig{},
	}

	cmd := &cobra.Command{
		Use:     "list-runs",
		Short:   "Get Job run details",
		Example: `optimus job runs <sample_job_goes_here> [--project \"project-id\"] [--start_date \"2006-01-02T15:04:05Z07:00\" --end_date \"2006-01-02T15:04:05Z07:00\"]`,
		Args:    cobra.MinimumNArgs(1),
		RunE:    run.RunE,
		PreRunE: run.PreRunE,
	}

	run.injectFlags(cmd)

	return cmd
}

func (r *runListCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&r.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringVar(&r.startDate, "start_date", "", "start date of job run")
	cmd.Flags().StringVar(&r.endDate, "end_date", "", "end date of job run")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&r.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&r.host, "host", "", "Optimus service endpoint url")
}

func (r *runListCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	if err := r.loadConfig(); err != nil {
		return err
	}

	if r.clientConfig == nil {
		r.logger = logger.NewDefaultLogger()
		cmd.MarkFlagRequired("project-name")
		cmd.MarkFlagRequired("host")
		return nil
	}

	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	if r.projectName == "" {
		r.projectName = r.clientConfig.Project.Name
	}
	if r.host == "" {
		r.host = r.clientConfig.Host
	}

	return nil
}

func (r *runListCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	r.logger.Info(fmt.Sprintf("Requesting status for project %s, job %s from %s",
		r.projectName, jobName, r.host))

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
	conn, err := connectivity.NewConnectivity(r.host, jobStatusTimeout)
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
	r.logger.Info(logger.ColoredSuccess("\nFound %d jobRun instances.", len(jobRuns)))
	return nil
}

func (r *runListCommand) createJobRunRequest(jobName, startDate, endDate string) (*pb.JobRunRequest, error) {
	var req *pb.JobRunRequest
	if startDate == "" && endDate == "" {
		req = &pb.JobRunRequest{
			ProjectName: r.projectName,
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
		ProjectName: r.projectName,
		JobName:     jobName,
		StartDate:   sDate,
		EndDate:     eDate,
	}
	return req, nil
}

func (*runListCommand) validateDateArgs(startDate, endDate string) error {
	if startDate == "" && endDate != "" {
		return errors.New("please provide the start date")
	}
	if startDate != "" && endDate == "" {
		return errors.New("please provide the end date")
	}
	return nil
}

func (r *runListCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(r.configFilePath)
	if err != nil {
		if errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
			r.clientConfig = nil
			return nil
		}
		return err
	}
	*r.clientConfig = *c
	return nil
}
