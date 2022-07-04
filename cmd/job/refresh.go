package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	saltConfig "github.com/odpf/salt/config"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/deploy"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	refreshTimeout = time.Minute * 30
	deployTimeout  = time.Minute * 30
	pollInterval   = time.Second * 15
)

type refreshCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	verbose                bool
	selectedNamespaceNames []string
	selectedJobNames       []string

	projectName string
	host        string

	refreshCounter        int
	refreshSuccessCounter int
	refreshFailedCounter  int
	deployCounter         int
	deploySuccessCounter  int
	deployFailedCounter   int
}

// NewRefreshCommand initializes command for refreshing job specification
func NewRefreshCommand() *cobra.Command {
	refresh := &refreshCommand{
		clientConfig: &config.ClientConfig{},
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

func (r *refreshCommand) RunE(_ *cobra.Command, _ []string) error {
	if len(r.selectedNamespaceNames) > 0 || len(r.selectedJobNames) > 0 {
		r.logger.Info("Refreshing job dependencies of selected jobs/namespaces")
	} else {
		r.logger.Info(fmt.Sprintf("Refreshing job dependencies of all jobs in %s", r.projectName))
	}

	start := time.Now()
	if err := r.refreshJobSpecificationRequest(); err != nil {
		return err
	}
	r.logger.Info(logger.ColoredSuccess("Job refresh & deployment finished, took %s", time.Since(start).Round(time.Second)))
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

func (r *refreshCommand) getRefreshDeploymentID(stream pb.JobSpecificationService_RefreshJobsClient) (deployID string, streamError error) {
	r.resetCounters()
	defer r.resetCounters()

	var refreshErrors []error
	for {
		response, err := stream.Recv()
		if err != nil {
			return "", err
		}

		switch response.Type {
		case models.ProgressTypeJobDependencyResolution:
			r.refreshCounter++
			if !response.GetSuccess() {
				r.refreshFailedCounter++
				if r.verbose {
					r.logger.Warn(logger.ColoredError("error '%s': failed to refresh dependency, %s", response.GetJobName(), response.GetValue()))
				}
				refreshErrors = append(refreshErrors, fmt.Errorf("failed to refresh: %s, %s", response.GetJobName(), response.GetValue()))
			} else {
				r.refreshSuccessCounter++
				if r.verbose {
					r.logger.Info(fmt.Sprintf("info '%s': dependency is successfully refreshed", response.GetJobName()))
				}
			}
		case models.ProgressTypeJobDeploymentRequestCreated:
			if len(refreshErrors) > 0 {
				r.logger.Error(logger.ColoredError("Refreshed %d/%d jobs.", r.refreshSuccessCounter, r.refreshSuccessCounter+r.refreshFailedCounter))
				for _, reqErr := range refreshErrors {
					r.logger.Error(logger.ColoredError(reqErr.Error()))
				}
			} else {
				r.logger.Info(logger.ColoredSuccess("Refreshed %d jobs.", r.refreshSuccessCounter))
			}

			if !response.GetSuccess() {
				r.logger.Warn(logger.ColoredError("Unable to request job deployment"))
			} else {
				r.logger.Info(logger.ColoredNotice("Deployment request created with ID: %s", response.GetValue()))
			}
			deployID = response.Value
			return
		default:
			if r.verbose {
				// ordinary progress event
				r.logger.Info(fmt.Sprintf("info '%s': %s", response.GetJobName(), response.GetValue()))
			}
		}
	}
}

func (r *refreshCommand) resetCounters() {
	r.refreshCounter = 0
	r.refreshSuccessCounter = 0
	r.refreshFailedCounter = 0
	r.deployCounter = 0
	r.deploySuccessCounter = 0
	r.deployFailedCounter = 0
}

func (r *refreshCommand) loadConfig() error {
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
