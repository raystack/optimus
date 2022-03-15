package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/odpf/optimus/config"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	refreshTimeout = time.Minute * 15
)

func jobRefreshCommand(l log.Logger, conf config.Optimus) *cli.Command {
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

	namespaces := cmd.Flags().StringArrayP("namespaces", "n", []string{}, "Namespace of Optimus project")
	jobs := cmd.Flags().StringArrayP("jobs", "j", []string{}, "Job names")

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
			l.Info(fmt.Sprintf("Refreshing job dependencies of selected jobs / namespaces"))
		}
		l.Info(fmt.Sprintf("Redeploy all jobs in %s project", projectName))
		start := time.Now()

		if err := refreshJobSpecificationRequest(l, projectName, *namespaces, *jobs, conf.Host, verbose); err != nil {
			return err
		}
		l.Info(coloredSuccess("Jobs refreshed successfully, took %s", time.Since(start).Round(time.Second)))
		return nil
	}
	return cmd
}

func refreshJobSpecificationRequest(l log.Logger, projectName string, namespaces []string, jobs []string,
	host string, verbose bool) (err error) {
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

	runtime := pb.NewRuntimeServiceClient(conn)
	respStream, err := runtime.RefreshJobs(refreshTimeoutCtx, &pb.RefreshJobsRequest{
		ProjectName:   projectName,
		NamespaceJobs: namespaceJobs,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Refresh process took too long, timing out"))
		}
		return errors.Wrapf(err, "Refresh request failed")
	}

	ackCounter := 0
	failedCounter := 0

	var refreshErrors []string
	var streamError error
	for {
		resp, err := respStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			streamError = err
			break
		}
		if resp.Ack {
			// ack for the job spec
			if !resp.GetSuccess() {
				failedCounter++
				refreshErrors = append(refreshErrors, fmt.Sprintf("failed to validate: %s, %s\n", resp.GetJobName(), resp.GetMessage()))
			}
			ackCounter++
			if verbose {
				l.Info(fmt.Sprintf("%d. %s successfully refreshed", ackCounter, resp.GetJobName()))
			}
		} else {
			if verbose {
				// ordinary progress event
				l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
			}
		}
	}

	if len(refreshErrors) > 0 {
		if verbose {
			for i, reqErr := range refreshErrors {
				l.Error(fmt.Sprintf("%d. %s", i+1, reqErr))
			}
		}
	} else if streamError != nil && failedCounter == 0 {
		// notify warnings if any.
		l.Warn(coloredNotice("request ended with warning"), "err", streamError)
		return nil
	}
	return streamError
}
