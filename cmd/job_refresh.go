package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	refreshTimeout = time.Minute * 15
	deployTimeout  = time.Hour * 3
	pollInterval   = time.Second * 15
)

func jobRefreshCommand(conf *config.ClientConfig) *cli.Command {
	var (
		projectName string
		verbose     bool
		namespaces  []string
		jobs        []string
		cmd         = &cli.Command{
			Use:     "refresh",
			Short:   "Refresh job deployments",
			Long:    "Redeploy jobs based on current server state",
			Example: "optimus job refresh",
		}
	)

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringSliceVarP(&namespaces, "namespaces", "N", nil, "Namespaces of Optimus project")
	cmd.Flags().StringSliceVarP(&jobs, "jobs", "J", nil, "Job names")

	cmd.RunE = func(c *cli.Command, args []string) error {
		l := initClientLogger(conf.Log)

		projectName = conf.Project.Name
		if projectName == "" {
			return fmt.Errorf("project configuration is required")
		}

		if len(namespaces) > 0 || len(jobs) > 0 {
			l.Info("Refreshing job dependencies of selected jobs/namespaces")
		}
		l.Info(fmt.Sprintf("Redeploying all jobs in %s project", projectName))
		start := time.Now()

		deployID, err := refreshJobSpecificationRequest(l, projectName, namespaces, jobs, conf.Host, verbose)
		if err != nil {
			return err
		}

		for keepPolling, timeout := true, time.After(deployTimeout); keepPolling; {
			isFinished, err := pollJobDeployment(l, deployID, conf.Host)
			if err != nil {
				return err
			}

			if isFinished {
				return nil
			}

			time.Sleep(pollInterval)

			select {
			case <-timeout:
				keepPolling = false
			default:
			}
		}

		l.Info(coloredSuccess("Job refresh & deployment finished, took %s", time.Since(start).Round(time.Second)))
		return nil
	}
	return cmd
}

func refreshJobSpecificationRequest(l log.Logger, projectName string, namespaces, jobs []string, host string, verbose bool,
) (deployID string, err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return "", err
	}
	defer conn.Close()

	refreshTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer deployCancel()

	jobSpecService := pb.NewJobSpecificationServiceClient(conn)
	respStream, err := jobSpecService.RefreshJobs(refreshTimeoutCtx, &pb.RefreshJobsRequest{
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
			if !resp.GetSuccess() {
				l.Warn(coloredError("unable to request job deployment"))
			} else {
				l.Info(fmt.Sprintf("Deployment request created with ID: %s", resp.GetValue()))
			}
			return resp.Value, nil
		default:
			if verbose {
				// ordinary progress event
				l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetValue()))
			}
		}
	}

	if len(refreshErrors) > 0 {
		l.Error(coloredError(fmt.Sprintf("Refreshed %d/%d jobs.", refreshSuccessCounter, refreshSuccessCounter+refreshFailedCounter)))
		for _, reqErr := range refreshErrors {
			l.Error(coloredError(reqErr))
		}
	} else {
		l.Info(coloredSuccess("Refreshed %d jobs.", refreshSuccessCounter))
	}

	return "", streamError
}

func pollJobDeployment(l log.Logger, deployID string, host string) (isFinished bool, err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return false, err
	}
	defer conn.Close()

	refreshTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer deployCancel()

	jobSpecService := pb.NewJobSpecificationServiceClient(conn)
	resp, err := jobSpecService.GetDeployJobsStatus(refreshTimeoutCtx, &pb.GetDeployJobsStatusRequest{
		DeployId: deployID,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Get deployment process took too long, timing out"))
		}
		return false, fmt.Errorf("getting deployment status failed: %w", err)
	}

	switch resp.Status {
	case models.JobDeploymentStatusInProgress.String():
		l.Info("Deployment is in progress...")
		return false, nil
	case models.JobDeploymentStatusInQueue.String():
		l.Info("Deployer is busy. Deployment request is still in queue...")
		return false, nil
	case models.JobDeploymentStatusCancelled.String():
		l.Error("Deployment request is cancelled. Deployer queue might be full.")
		return false, nil
	}

	if len(resp.Failures) > 0 {
		l.Error(coloredError("Unable to deploy below jobs:"))
		for i, failedJob := range resp.Failures {
			l.Error(coloredError("%d. %s: %s", i+1, failedJob.GetJobName(), failedJob.GetMessage()))
		}
	} else {
		l.Info(coloredSuccess("All jobs deployed successfully."))
	}
	l.Info(coloredSuccess("Deployed %d jobs", resp.TotalSucceed))

	return true, nil
}
