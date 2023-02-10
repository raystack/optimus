package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/deploy"
	"github.com/odpf/optimus/client/cmd/internal"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	refreshTimeout = time.Minute * 60
	deployTimeout  = time.Minute * 30
	pollInterval   = time.Second * 15
)

type refreshCommand struct {
	logger         log.Logger
	configFilePath string

	verbose                bool
	selectedNamespaceNames []string
	selectedJobNames       []string

	projectName string
	host        string
}

// NewRefreshCommand initializes command for refreshing job specification
func NewRefreshCommand() *cobra.Command {
	refresh := &refreshCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "refresh",
		Short:   "Refresh job deployments",
		Long:    "Redeploy jobs based on current server state",
		Example: "optimus job refresh",
		RunE:    refresh.RunE,
		PreRunE: refresh.PreRunE,
	}

	refresh.injectFlags(cmd)
	return cmd
}

func (r *refreshCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&r.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().BoolVarP(&r.verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringSliceVarP(&r.selectedNamespaceNames, "namespaces", "N", nil, "Namespaces of Optimus project")
	cmd.Flags().StringSliceVarP(&r.selectedJobNames, "jobs", "J", nil, "Job names")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&r.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&r.host, "host", "", "Optimus service endpoint url")
}

func (r *refreshCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(r.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if r.projectName == "" {
		r.projectName = conf.Project.Name
	}
	if r.host == "" {
		r.host = conf.Host
	}
	return nil
}

func (r *refreshCommand) RunE(_ *cobra.Command, _ []string) error {
	if len(r.selectedNamespaceNames) > 0 || len(r.selectedJobNames) > 0 {
		r.logger.Info("Refreshing job dependencies of selected jobs/namespaces")
	} else {
		r.logger.Info("Refreshing job dependencies of all jobs in %s", r.projectName)
	}

	start := time.Now()
	if err := r.refreshJobSpecificationRequest(); err != nil {
		return err
	}
	r.logger.Info("Job refresh & deployment finished, took %s", time.Since(start).Round(time.Second))
	return nil
}

func (r *refreshCommand) refreshJobSpecificationRequest() error {
	conn, err := connectivity.NewConnectivity(r.host, refreshTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	jobSpecService := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	respStream, err := jobSpecService.RefreshJobs(conn.GetContext(), &pb.RefreshJobsRequest{
		ProjectName:    r.projectName,
		NamespaceNames: r.selectedNamespaceNames,
		JobNames:       r.selectedJobNames,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.logger.Error("Refresh process took too long, timing out")
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
			if r.verbose {
				logger.PrintLogStatusVerbose(r.logger, logStatus)
			} else {
				logger.PrintLogStatus(r.logger, logStatus)
			}
			continue
		}

		deployID = resp.GetDeploymentId()
	}

	return deployID, nil
}
