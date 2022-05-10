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
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const refreshTimeout = time.Minute * 15

type refreshCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	verbose    bool
	namespaces []string
	jobs       []string

	refreshCounter        int
	refreshSuccessCounter int
	refreshFailedCounter  int
	deployCounter         int
	deploySuccessCounter  int
	deployFailedCounter   int
}

// NewRefreshCommand initializes command for rendering job specification
func NewRefreshCommand(clientConfig *config.ClientConfig) *cobra.Command {
	render := &refreshCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>]",
		RunE:    render.RunE,
		PreRunE: render.PreRunE,
	}
	cmd.Flags().BoolVarP(&render.verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringSliceVarP(&render.namespaces, "namespaces", "N", nil, "Namespaces of Optimus project")
	cmd.Flags().StringSliceVarP(&render.jobs, "jobs", "J", nil, "Job names")
	return cmd
}

func (r *refreshCommand) PreRunE(cmd *cobra.Command, args []string) error {
	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	return nil
}

func (r *refreshCommand) RunE(cmd *cobra.Command, args []string) error {
	if len(r.namespaces) > 0 || len(r.jobs) > 0 {
		r.logger.Info("Refreshing job dependencies of selected jobs/namespaces")
	}

	projectName := r.clientConfig.Project.Name
	if projectName == "" {
		return fmt.Errorf("project configuration is required")
	}
	r.logger.Info(fmt.Sprintf("Redeploying all jobs in %s project", projectName))
	start := time.Now()

	if err := r.refreshJobSpecificationRequest(); err != nil {
		return err
	}
	r.logger.Info("Job refresh & deployment finished, took %s", time.Since(start).Round(time.Second))
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
		NamespaceNames: r.namespaces,
		JobNames:       r.jobs,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.logger.Error("Refresh process took too long, timing out")
		}
		return fmt.Errorf("refresh request failed: %w", err)
	}
	return r.getRefreshJobsResponse(respStream)
}

func (r *refreshCommand) getRefreshJobsResponse(stream pb.JobSpecificationService_RefreshJobsClient) error {
	r.resetCounters()
	defer r.resetCounters()

	var allRefreshErrors []error
	var allDeployErrors []error
	var streamError error
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			streamError = err
			break
		}

		refreshErrs, deployErrs := r.handleStreamResponse(resp)
		allRefreshErrors = append(refreshErrs, refreshErrs...)
		allDeployErrors = append(allDeployErrors, deployErrs...)
	}

	if len(allRefreshErrors) > 0 {
		r.logger.Error(fmt.Sprintf("Refreshed %d/%d jobs.",
			r.refreshSuccessCounter, r.refreshSuccessCounter+r.refreshFailedCounter))
		for _, reqErr := range allRefreshErrors {
			r.logger.Error(reqErr.Error())
		}
	} else {
		r.logger.Info("Refreshed %d jobs.", r.refreshSuccessCounter)
	}

	if len(allDeployErrors) > 0 {
		r.logger.Error("Deployed %d/%d jobs.",
			r.deploySuccessCounter, r.deploySuccessCounter+r.deployFailedCounter)
		for _, reqErr := range allDeployErrors {
			r.logger.Error(reqErr.Error())
		}
	} else {
		r.logger.Info("Deployed %d jobs.", r.deploySuccessCounter)
	}

	return streamError
}

func (r *refreshCommand) handleStreamResponse(refreshResponse *pb.RefreshJobsResponse) (refreshErrs, deployErrs []error) {
	switch refreshResponse.Type {
	case models.ProgressTypeJobUpload:
		r.deployCounter++
		if !refreshResponse.GetSuccess() {
			r.deployFailedCounter++
			if r.verbose {
				r.logger.Warn(fmt.Sprintf("%d. %s failed to be deployed: %s", r.deployCounter, refreshResponse.GetJobName(), refreshResponse.GetMessage()))
			}
			deployErrs = append(deployErrs, fmt.Errorf("failed to deploy: %s, %s", refreshResponse.GetJobName(), refreshResponse.GetMessage()))
		} else {
			r.deploySuccessCounter++
			if r.verbose {
				r.logger.Info(fmt.Sprintf("%d. %s successfully deployed", r.deployCounter, refreshResponse.GetJobName()))
			}
		}
	case models.ProgressTypeJobDependencyResolution:
		r.refreshCounter++
		if !refreshResponse.GetSuccess() {
			r.refreshFailedCounter++
			if r.verbose {
				r.logger.Warn(fmt.Sprintf("error '%s': failed to refresh dependency, %s", refreshResponse.GetJobName(), refreshResponse.GetMessage()))
			}
			refreshErrs = append(refreshErrs, fmt.Errorf("failed to refresh: %s, %s", refreshResponse.GetJobName(), refreshResponse.GetMessage()))
		} else {
			r.refreshSuccessCounter++
			if r.verbose {
				r.logger.Info(fmt.Sprintf("info '%s': dependency is successfully refreshed", refreshResponse.GetJobName()))
			}
		}
	default:
		if r.verbose {
			// ordinary progress event
			r.logger.Info(fmt.Sprintf("info '%s': %s", refreshResponse.GetJobName(), refreshResponse.GetMessage()))
		}
	}
	return
}

func (r *refreshCommand) resetCounters() {
	r.refreshCounter = 0
	r.refreshSuccessCounter = 0
	r.refreshFailedCounter = 0
	r.deployCounter = 0
	r.deploySuccessCounter = 0
	r.deployFailedCounter = 0
}
