package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

const runJobTimeout = time.Minute * 1

type runCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig
}

// NewRunCommand initializes run command
func NewRunCommand(logger log.Logger, clientConfig *config.ClientConfig) *cobra.Command {
	run := &runCommand{
		logger:       logger,
		clientConfig: clientConfig,
	}
	cmd := &cobra.Command{
		Use:     "run",
		Short:   "[EXPERIMENTAL] run the provided job on optimus cluster",
		Args:    cobra.MinimumNArgs(1),
		Example: "optimus job run <job_name>",
		Hidden:  true,
		RunE:    run.RunE,
	}
	cmd.Flags().StringP("namespace", "n", "", "Namespace of the resource within project")
	cmd.MarkFlagRequired("namespace")
	return cmd
}

func (r *runCommand) RunE(cmd *cobra.Command, args []string) error {
	namespaceName, _ := cmd.Flags().GetString("namespace")

	namespace, err := r.clientConfig.GetNamespaceByName(namespaceName)
	if err != nil {
		return err
	}

	pluginRepo := models.PluginRegistry
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
	jobSpecRepo := local.NewJobSpecRepository(
		jobSpecFs,
		local.NewJobSpecAdapter(pluginRepo),
	)

	jobSpec, err := jobSpecRepo.GetByName(args[0])
	if err != nil {
		return err
	}
	return r.runJobSpecificationRequest(namespace.Name, jobSpec)
}

func (r *runCommand) runJobSpecificationRequest(namespaceName string, jobSpec models.JobSpec) error {
	conn, err := connectivity.NewConnectivity(r.clientConfig.Host, runJobTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	pluginRepo := models.PluginRegistry
	adapt := v1handler.NewAdapter(pluginRepo, nil)
	adaptedSpec := adapt.ToJobProto(jobSpec)

	r.logger.Info("please wait...")
	jobRun := pb.NewJobRunServiceClient(conn.GetConnection())
	jobResponse, err := jobRun.RunJob(conn.GetContext(), &pb.RunJobRequest{
		ProjectName:   r.clientConfig.Project.Name,
		NamespaceName: namespaceName,
		Specifications: []*pb.JobSpecification{
			adaptedSpec,
		},
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.logger.Info("process took too long, timing out")
		}
		return fmt.Errorf("request failed for job %s: %w", jobSpec.Name, err)
	}
	r.logger.Info(jobResponse.String())
	return nil
}
