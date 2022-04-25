package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	refreshTimeout = time.Minute * 30
	deployTimeout  = time.Minute * 30
	pollInterval   = time.Second * 15
)

func jobRefreshCommand(conf *config.ClientConfig) *cli.Command {
	var (
		projectName            string
		verbose                bool
		selectedNamespaceNames []string
		jobs                   []string
		cmd                    = &cli.Command{
			Use:     "refresh",
			Short:   "Refresh job deployments",
			Long:    "Redeploy jobs based on current server state",
			Example: "optimus job refresh",
		}
	)

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringSliceVarP(&selectedNamespaceNames, "namespace-names", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().StringSliceVarP(&jobs, "jobs", "J", nil, "Job names")

	cmd.RunE = func(c *cli.Command, args []string) error {
		l := initClientLogger(conf.Log)

		projectName = conf.Project.Name
		if projectName == "" {
			return fmt.Errorf("project configuration is required")
		}

		if len(selectedNamespaceNames) > 0 || len(jobs) > 0 {
			l.Info("Refreshing job dependencies of selected jobs/namespaces in %s", projectName)
		} else {
			l.Info(fmt.Sprintf("Refreshing job dependencies of all jobs in %s", projectName))
		}

		start := time.Now()

		ctx, conn, closeConn, err := initClientConnection(conf.Host, refreshTimeout)
		if err != nil {
			return err
		}
		defer closeConn()

		jobSpecService := pb.NewJobSpecificationServiceClient(conn)
		deployID, err := refreshJobSpecificationRequest(ctx, l, jobSpecService, projectName, selectedNamespaceNames, jobs, verbose)
		if err != nil {
			return err
		}

		l.Info(fmt.Sprintf("\nRedeploying all jobs in %s project", projectName))
		if err := pollJobDeployment(ctx, l, jobSpecService, deployID); err != nil {
			return err
		}

		l.Info(coloredNotice("\nJob refresh & deployment finished, took %s", time.Since(start).Round(time.Second)))
		return nil
	}
	return cmd
}

func refreshJobSpecificationRequest(ctx context.Context, l log.Logger, jobSpecService pb.JobSpecificationServiceClient, projectName string, namespaces, jobs []string, verbose bool,
) (deployID string, err error) {
	respStream, err := jobSpecService.RefreshJobs(ctx, &pb.RefreshJobsRequest{
		ProjectName:    projectName,
		NamespaceNames: namespaces,
		JobNames:       jobs,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Refresh process took too long, timing out"))
		}
		return "", fmt.Errorf("refresh request failed: %w", err)
	}

	var refreshErrors []string
	refreshCounter, refreshSuccessCounter, refreshFailedCounter := 0, 0, 0

	var streamError error
	for {
		resp, err := respStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			streamError = err
			break
		}

		switch resp.Type {
		case models.ProgressTypeJobDependencyResolution:
			refreshCounter++
			if !resp.GetSuccess() {
				refreshFailedCounter++
				if verbose {
					l.Warn(coloredError(fmt.Sprintf("error '%s': failed to refresh dependency, %s", resp.GetJobName(), resp.GetValue())))
				}
				refreshErrors = append(refreshErrors, fmt.Sprintf("failed to refresh: %s, %s", resp.GetJobName(), resp.GetValue()))
			} else {
				refreshSuccessCounter++
				if verbose {
					l.Info(fmt.Sprintf("info '%s': dependency is successfully refreshed", resp.GetJobName()))
				}
			}
		case models.ProgressTypeJobDeploymentRequestCreated:
			if len(refreshErrors) > 0 {
				l.Error(coloredError(fmt.Sprintf("Refreshed %d/%d jobs.", refreshSuccessCounter, refreshSuccessCounter+refreshFailedCounter)))
				for _, reqErr := range refreshErrors {
					l.Error(coloredError(reqErr))
				}
			} else {
				l.Info(coloredSuccess("Refreshed %d jobs.", refreshSuccessCounter))
			}

			if !resp.GetSuccess() {
				l.Warn(coloredError("Unable to request job deployment"))
			} else {
				l.Info(coloredNotice(fmt.Sprintf("Deployment request created with ID: %s", resp.GetValue())))
			}

			return resp.Value, nil
		default:
			if verbose {
				// ordinary progress event
				l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetValue()))
			}
		}
	}

	return "", streamError
}

func pollJobDeployment(ctx context.Context, l log.Logger, jobSpecService pb.JobSpecificationServiceClient, deployID string) error {
	for keepPolling, timeout := true, time.After(deployTimeout); keepPolling; {
		resp, err := jobSpecService.GetDeployJobsStatus(ctx, &pb.GetDeployJobsStatusRequest{
			DeployId: deployID,
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Get deployment process took too long, timing out"))
			}
			return fmt.Errorf("getting deployment status failed: %w", err)
		}

		switch resp.Status {
		case models.JobDeploymentStatusInProgress.String():
			l.Info("Deployment is in progress...")
		case models.JobDeploymentStatusInQueue.String():
			l.Info("Deployment request is in queue...")
		case models.JobDeploymentStatusCancelled.String():
			l.Error("Deployment request is cancelled.")
			return nil
		case models.JobDeploymentStatusSucceed.String():
			l.Info(coloredSuccess("Deployed %d jobs", resp.SuccessCount))
			return nil
		case models.JobDeploymentStatusFailed.String():
			if resp.FailureCount > 0 {
				l.Error(coloredError("Unable to deploy below jobs:"))
				for i, failedJob := range resp.Failures {
					l.Error(coloredError("%d. %s: %s", i+1, failedJob.GetJobName(), failedJob.GetMessage()))
				}
			}
			l.Error(coloredError("Deployed %d/%d jobs.", resp.SuccessCount, resp.SuccessCount+resp.FailureCount))
			return nil
		}

		time.Sleep(pollInterval)

		select {
		case <-timeout:
			keepPolling = false
		default:
		}
	}
	return nil
}
