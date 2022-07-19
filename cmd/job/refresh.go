package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/deploy"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
)

const (
	refreshTimeout = time.Minute * 30
	deployTimeout  = time.Minute * 30
	pollInterval   = time.Second * 15
)

type refreshCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	verbose                bool
	selectedNamespaceNames []string
	selectedJobNames       []string

	refreshCounter        int
	refreshSuccessCounter int
	refreshFailedCounter  int
	deployCounter         int
	deploySuccessCounter  int
	deployFailedCounter   int
}

// NewRefreshCommand initializes command for refreshing job specification
func NewRefreshCommand(clientConfig *config.ClientConfig) *cobra.Command {
	render := &refreshCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "refresh",
		Short:   "Refresh job deployments",
		Long:    "Redeploy jobs based on current server state",
		Example: "optimus job refresh",
		RunE:    render.RunE,
		PreRunE: render.PreRunE,
	}
	cmd.Flags().BoolVarP(&render.verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringSliceVarP(&render.selectedNamespaceNames, "namespaces", "N", nil, "Namespaces of Optimus project")
	cmd.Flags().StringSliceVarP(&render.selectedJobNames, "jobs", "J", nil, "Job names")
	return cmd
}

func (r *refreshCommand) PreRunE(_ *cobra.Command, _ []string) error {
	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	return nil
}

func (r *refreshCommand) RunE(_ *cobra.Command, _ []string) error {
	projectName := r.clientConfig.Project.Name
	if projectName == "" {
		return fmt.Errorf("project configuration is required")
	}
	if len(r.selectedNamespaceNames) > 0 || len(r.selectedJobNames) > 0 {
		r.logger.Info("Refreshing job dependencies of selected jobs/namespaces")
	} else {
		r.logger.Info(fmt.Sprintf("Refreshing job dependencies of all jobs in %s", projectName))
	}

	start := time.Now()
	if err := r.refreshJobSpecificationRequest(); err != nil {
		return err
	}
	r.logger.Info(logger.ColoredSuccess("Job refresh & deployment finished, took %s", time.Since(start).Round(time.Second)))
	return nil
}

func (r *refreshCommand) refreshJobSpecificationRequest() error {
	conn, err := connectivity.NewConnectivity(r.clientConfig.Host, refreshTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	jobSpecService := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	respStream, err := jobSpecService.RefreshJobs(conn.GetContext(), &pb.RefreshJobsRequest{
		ProjectName:    r.clientConfig.Project.Name,
		NamespaceNames: r.selectedNamespaceNames,
		JobNames:       r.selectedJobNames,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.logger.Error(logger.ColoredError("Refresh process took too long, timing out"))
		}
		return fmt.Errorf("refresh request failed: %w", err)
	}

	deployID, err := r.getRefreshDeploymentID(respStream)
	if err != nil {
		return err
	}
	return deploy.PollJobDeployment(conn.GetContext(), r.logger, jobSpecService, deployTimeout, pollInterval, deployID)
}

func (r *refreshCommand) getRefreshDeploymentID(stream pb.JobSpecificationService_RefreshJobsClient) (string, error) {
	deployID := ""

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return deployID, err
		}

		if logStatus := resp.GetLogStatus(); logStatus != nil {
			switch logStatus.GetLevel() {
			case pb.Level_Info:
				r.logger.Info(logStatus.GetMessage())
			case pb.Level_Warning:
				r.logger.Warn(logStatus.GetMessage())
			case pb.Level_Error:
				r.logger.Error(logStatus.GetMessage())
			}
			continue
		}

		deployID = resp.GetDeploymentId()
	}

	return deployID, nil
}
