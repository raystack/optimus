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
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/namespace"
	"github.com/odpf/optimus/config"
)

const registerTimeout = time.Minute * 15

type registerCommand struct {
	logger log.Logger
}

// NewRegisterCommand initializes command to create a project
func NewRegisterCommand(logger log.Logger) *cobra.Command {
	register := &registerCommand{
		logger: logger,
	}

	cmd := &cobra.Command{
		Use:     "register",
		Short:   "Register project if it does not exist and update if it does",
		Example: "optimus project register [--flag]",
		RunE:    register.RunE,
	}
	cmd.Flags().String("dir", "", "Directory where the Optimus client config resides")
	cmd.Flags().Bool("with-namespaces", false, "If yes, then namespace will be registered or updated as well")
	return cmd
}

func (r *registerCommand) RunE(cmd *cobra.Command, args []string) error {
	dirPath, _ := cmd.Flags().GetString("dir")
	withNamespaces, _ := cmd.Flags().GetBool("with-namespaces")

	filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
	clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
	if err != nil {
		return err
	}
	r.logger.Info(fmt.Sprintf("Registering project [%s] to server [%s]", clientConfig.Project.Name, clientConfig.Host))
	if err := r.registerProject(clientConfig.Host, clientConfig.Project); err != nil {
		return err
	}
	if withNamespaces {
		r.logger.Info(fmt.Sprintf("Registering all namespaces from: %s", filePath))
		if err := namespace.RegisterSelectedNamespaces(r.logger, clientConfig.Host, clientConfig.Project.Name, clientConfig.Namespaces...); err != nil {
			return err
		}
	}
	return nil
}

func (r *registerCommand) registerProject(serverHost string, project config.Project) error {
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
			r.logger.Warn(fmt.Sprintf("Ignoring project config changes: %v", err))
			return nil
		}
		return fmt.Errorf("failed to register or update project: %w", err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to register or update project, %s", registerResponse.Message)
	}
	r.logger.Info("Project registration finished successfully")
	return nil
}
