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
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
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
		clientConfig: &config.ClientConfig{},
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
	if err := v.loadConfig(); err != nil {
		return err
	}
	v.logger = logger.NewClientLogger(v.clientConfig.Log)
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
	v.logger.Info(fmt.Sprintf("Validating job specifications for project: %s, namespace: %s", projectName, namespace.Name))
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return fmt.Errorf("directory '%s': %w", namespace.Job.Path, err)
	}
	if err := v.validateJobSpecificationRequest(jobSpecs); err != nil {
		return err
	}
	v.logger.Info(logger.ColoredSuccess("Jobs validated successfully, took %s", time.Since(start).Round(time.Second)))
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
			v.logger.Error(logger.ColoredError("Validate process took too long, timing out"))
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

		if logStatus := resp.GetLogStatus(); logStatus != nil {
			if !v.verbose {
				continue
			}
			switch logStatus.GetLevel() {
			case pb.Level_Info:
				v.logger.Info(logStatus.GetMessage())
			case pb.Level_Warning:
				v.logger.Warn(logStatus.GetMessage())
			case pb.Level_Error:
				v.logger.Error(logStatus.GetMessage())
			}
			continue
		}
	}

	return nil
}

func (v *validateCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	conf, err := config.LoadClientConfig(v.configFilePath)
	if err != nil {
		return err
	}
	*v.clientConfig = *conf
	return nil
}
