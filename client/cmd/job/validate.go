package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const validateTimeout = time.Minute * 5

type validateCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	verbose       bool
	namespaceName string
}

// NewValidateCommand initializes command for validating job specification
func NewValidateCommand() *cobra.Command {
	validate := &validateCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "validate",
		Short:   "Run basic checks on all jobs",
		Long:    "Check if specifications are valid for deployment",
		Example: "optimus job validate",
		RunE:    validate.RunE,
		PreRunE: validate.PreRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&validate.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().BoolVarP(&validate.verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringVarP(&validate.namespaceName, "namespace", "n", validate.namespaceName, "Namespace of the resource within project")
	cmd.MarkFlagRequired("namespace")
	return cmd
}

func (v *validateCommand) PreRunE(_ *cobra.Command, _ []string) error { // Load mandatory config
	conf, err := config.LoadClientConfig(v.configFilePath)
	if err != nil {
		return err
	}
	v.clientConfig = conf
	return nil
}

func (v *validateCommand) RunE(_ *cobra.Command, _ []string) error {
	namespace, err := v.clientConfig.GetNamespaceByName(v.namespaceName)
	if err != nil {
		return err
	}

	pluginRepo := models.PluginRegistry
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
	jobSpecRepo := local.NewJobSpecRepository(
		jobSpecFs,
		local.NewJobSpecAdapter(pluginRepo),
	)

	start := time.Now()
	projectName := v.clientConfig.Project.Name
	v.logger.Info("Validating job specifications for project: %s, namespace: %s", projectName, namespace.Name)
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return fmt.Errorf("directory '%s': %w", namespace.Job.Path, err)
	}
	if err := v.validateJobSpecificationRequest(jobSpecs); err != nil {
		return err
	}
	v.logger.Info("Jobs validated successfully, took %s", time.Since(start).Round(time.Second))
	return nil
}

func (v *validateCommand) validateJobSpecificationRequest(jobSpecs []models.JobSpec) error {
	conn, err := connectivity.NewConnectivity(v.clientConfig.Host, validateTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	adaptedJobSpecs := []*pb.JobSpecification{}
	for _, spec := range jobSpecs {
		adaptedSpec := v1handler.ToJobSpecificationProto(spec)
		adaptedJobSpecs = append(adaptedJobSpecs, adaptedSpec)
	}

	checkJobSpecRequest := &pb.CheckJobSpecificationsRequest{
		ProjectName:   v.clientConfig.Project.Name,
		Jobs:          adaptedJobSpecs,
		NamespaceName: v.namespaceName,
	}
	job := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	respStream, err := job.CheckJobSpecifications(conn.GetContext(), checkJobSpecRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			v.logger.Error("Validate process took too long, timing out")
		}
		return fmt.Errorf("validate request failed: %w", err)
	}
	return v.getCheckJobSpecificationsResponse(respStream)
}

func (v *validateCommand) getCheckJobSpecificationsResponse(stream pb.JobSpecificationService_CheckJobSpecificationsClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if logStatus := resp.GetLogStatus(); logStatus != nil && v.verbose {
			logger.PrintLogStatus(v.logger, logStatus)
			continue
		}
	}

	return nil
}
