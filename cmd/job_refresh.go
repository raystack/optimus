package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	refreshTimeout = time.Minute * 15
)

func jobRefreshCommand(l log.Logger, conf *config.ClientConfig) *cli.Command {
	var (
		projectName string
		verbose     bool
		cmd         = &cli.Command{
			Use:     "refresh",
			Short:   "Refresh job deployments",
			Long:    "Redeploy jobs based on current server state",
			Example: "optimus job refresh",
		}
	)

	cmd.Flags().StringVarP(&projectName, "project", "p", conf.Project.Name, "Optimus project name")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to operation")

	namespaces := cmd.Flags().StringArrayP("namespaces", "N", []string{}, "Namespaces of Optimus project")
	jobs := cmd.Flags().StringArrayP("jobs", "J", []string{}, "Job names")

	cmd.RunE = func(c *cli.Command, args []string) error {
		if projectName == "" {
			return fmt.Errorf("project configuration is required")
		}
		if len(*jobs) > 0 && len(*namespaces) > 1 {
			return fmt.Errorf("limit namespace to one or remove jobs to refresh the whole namespaces")
		}
		if len(*jobs) > 0 && len(*namespaces) == 0 {
			return fmt.Errorf("namespace is required to refresh selected jobs")
		}

		if len(*namespaces) > 0 || len(*jobs) > 0 {
			l.Info("Refreshing job dependencies of selected jobs/namespaces")
		}
		l.Info(fmt.Sprintf("Redeploying all jobs in %s project", projectName))
		start := time.Now()

		if err := refreshJobSpecificationRequest(l, projectName, *namespaces, *jobs, conf.Host, verbose); err != nil {
			return err
		}
		l.Info(coloredSuccess("Job refresh & deployment finished, took %s", time.Since(start).Round(time.Second)))
		return nil
	}
	return cmd
}

func refreshJobSpecificationRequest(l log.Logger, projectName string, namespaces, jobs []string, host string, verbose bool,
) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	refreshTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer deployCancel()

	var namespaceJobs []*pb.NamespaceJobs
	for _, namespace := range namespaces {
		namespaceJobs = append(namespaceJobs, &pb.NamespaceJobs{
			NamespaceName: namespace,
			JobNames:      jobs,
		})
	}

	jobSpecService := pb.NewJobSpecificationServiceClient(conn)
	respStream, err := jobSpecService.RefreshJobs(refreshTimeoutCtx, &pb.RefreshJobsRequest{
		ProjectName:   projectName,
		NamespaceJobs: namespaceJobs,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Refresh process took too long, timing out"))
		}
		return fmt.Errorf("refresh request failed: %w", err)
	}

	var refreshErrors []string
	refreshCounter, refreshSuccessCounter, refreshFailedCounter := 0, 0, 0

	var deployErrors []string
	deployCounter, deploySuccessCounter, deployFailedCounter := 0, 0, 0

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
		case models.ProgressTypeJobUpload:
			deployCounter++
			if !resp.GetSuccess() {
				deployFailedCounter++
				if verbose {
					l.Info(coloredError(fmt.Sprintf("%d. %s failed to be deployed: %s", deployCounter, resp.GetJobName(), resp.GetMessage())))
				}
				deployErrors = append(deployErrors, fmt.Sprintf("failed to deploy: %s, %s", resp.GetJobName(), resp.GetMessage()))
			} else {
				deploySuccessCounter++
				if verbose {
					l.Info(fmt.Sprintf("%d. %s successfully deployed", deployCounter, resp.GetJobName()))
				}
			}
		case models.ProgressTypeJobDependencyResolution:
			refreshCounter++
			if !resp.GetSuccess() {
				refreshFailedCounter++
				if verbose {
					l.Info(coloredError(fmt.Sprintf("error '%s': failed to refresh dependency, %s", resp.GetJobName(), resp.GetMessage())))
				}
				refreshErrors = append(refreshErrors, fmt.Sprintf("failed to refresh: %s, %s", resp.GetJobName(), resp.GetMessage()))
			} else {
				refreshSuccessCounter++
				if verbose {
					l.Info(fmt.Sprintf("info '%s': dependency is successfully refreshed", resp.GetJobName()))
				}
			}
		default:
			if verbose {
				// ordinary progress event
				l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
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

	if len(deployErrors) > 0 {
		l.Error(coloredError("Deployed %d/%d jobs.", deploySuccessCounter, deploySuccessCounter+deployFailedCounter))
		for _, reqErr := range deployErrors {
			l.Error(coloredError(reqErr))
		}
	} else {
		l.Info(coloredSuccess("Deployed %d jobs.", deploySuccessCounter))
	}

	return streamError
}
