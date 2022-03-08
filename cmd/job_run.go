package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	runJobTimeout = time.Minute * 1
)

func jobRunCommand(l log.Logger, jobSpecRepo JobSpecRepository, pluginRepo models.PluginRepository,
	conf config.Optimus) *cli.Command {
	var (
		projectName = conf.Project.Name
		namespace   = conf.Namespace.Name
	)
	cmd := &cli.Command{
		Use:     "run",
		Short:   "[EXPERIMENTAL] run the provided job on optimus cluster",
		Args:    cli.MinimumNArgs(1),
		Example: "optimus job run <job_name> [--project g-optimus]",
		Hidden:  true,
	}
	cmd.Flags().StringVarP(&projectName, "project", "p", projectName, "Project name of optimus managed repository")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", namespace, "Namespace of job that needs to run")

	cmd.RunE = func(c *cli.Command, args []string) error {
		jobSpec, err := jobSpecRepo.GetByName(args[0])
		if err != nil {
			return err
		}

		return runJobSpecificationRequest(l, projectName, namespace, conf.Host, jobSpec, pluginRepo)
	}
	return cmd
}

func runJobSpecificationRequest(l log.Logger, projectName, namespace, host string, jobSpec models.JobSpec, pluginRepo models.PluginRepository) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info(coloredError("can't reach optimus service"))
		}
		return err
	}
	defer conn.Close()

	runTimeoutCtx, runCancel := context.WithTimeout(context.Background(), runJobTimeout)
	defer runCancel()

	adapt := v1handler.NewAdapter(pluginRepo, nil)
	adaptedSpec, err := adapt.ToJobProto(jobSpec)
	if err != nil {
		return err
	}

	l.Info("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)
	jobResponse, err := runtime.RunJob(runTimeoutCtx, &pb.RunJobRequest{
		ProjectName:   projectName,
		NamespaceName: namespace,
		Specifications: []*pb.JobSpecification{
			adaptedSpec,
		},
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("process took too long, timing out")
		}
		return fmt.Errorf("request failed for job %s: %w", jobSpec.Name, err)
	}
	l.Info(fmt.Sprintf("%v", jobResponse))
	return nil
}
