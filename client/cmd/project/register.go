package project

import (
	"fmt"
	"path"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/namespace"
	"github.com/odpf/optimus/config"
)

const registerTimeout = time.Minute * 15

type registerCommand struct {
	logger log.Logger

	dirPath        string
	withNamespaces bool
}

// NewRegisterCommand initializes command to create a project
func NewRegisterCommand() *cobra.Command {
	register := &registerCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "register",
		Short:   "Register project if it does not exist and update if it does",
		Example: "optimus project register [--flag]",
		RunE:    register.RunE,
	}
	cmd.Flags().StringVar(&register.dirPath, "dir", register.dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().BoolVar(&register.withNamespaces, "with-namespaces", register.withNamespaces, "If yes, then namespace will be registered or updated as well")
	return cmd
}

func (r *registerCommand) RunE(_ *cobra.Command, _ []string) error {
	filePath := path.Join(r.dirPath, config.DefaultFilename)
	clientConfig, err := config.LoadClientConfig(filePath)
	if err != nil {
		return err
	}
	r.logger.Info("Registering project [%s] to server [%s]", clientConfig.Project.Name, clientConfig.Host)
	if err := RegisterProject(r.logger, clientConfig.Host, clientConfig.Project); err != nil {
		return err
	}
	if r.withNamespaces {
		r.logger.Info("Registering all namespaces from: %s", filePath)
		if err := namespace.RegisterSelectedNamespaces(r.logger, clientConfig.Host, clientConfig.Project.Name, clientConfig.Namespaces...); err != nil {
			return err
		}
	}
	return nil
}

// RegisterProject registers a project to the targeted server host
func RegisterProject(logger log.Logger, serverHost string, project config.Project) error {
	conn, err := connectivity.NewConnectivity(serverHost, registerTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	projectServiceClient := pb.NewProjectServiceClient(conn.GetConnection())
	projectSpec := &pb.ProjectSpecification{
		Name:   project.Name,
		Config: project.Config,
	}
	registerResponse, err := projectServiceClient.RegisterProject(conn.GetContext(), &pb.RegisterProjectRequest{
		Project: projectSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			logger.Warn(fmt.Sprintf("Ignoring project config changes: %v", err))
			return nil
		}
		return fmt.Errorf("failed to register or update project: %w", err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to register or update project, %s", registerResponse.Message)
	}
	logger.Info("Project registration finished successfully")
	return nil
}
