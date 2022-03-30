package cmd

import (
	"context"
	"fmt"
	"path"

	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
)

func projectCommand() *cli.Command {
	cmd := &cli.Command{
		Use:     "project",
		Short:   "Commands that will let the user to operate on project",
		Example: "optimus project [sub-command]",
	}
	cmd.AddCommand(projectRegisterCommand())
	return cmd
}

func projectRegisterCommand() *cli.Command {
	var dirPath string
	var withNamespaces bool
	cmd := &cli.Command{
		Use:     "register",
		Short:   "Register project if it does not exist and update if it does",
		Example: "optimus project register [--flag]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		filePath := path.Join(dirPath, config.DefaultConfigFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if err != nil {
			return err
		}
		l := initDefaultLogger()
		if err := registerProject(l, clientConfig); err != nil {
			return err
		}
		if withNamespaces {
			if err := registerAllNamespaces(l, clientConfig); err != nil {
				return err
			}
		}
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().BoolVar(&withNamespaces, "with-namespaces", withNamespaces, "If yes, then namespace will be registered or updated as well")
	return cmd
}

func registerProject(l log.Logger, clientConfig *config.ClientConfig) error {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	registerTimeoutCtx, registerCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer registerCancel()

	conn, err := createConnection(dialTimeoutCtx, clientConfig.Host)
	if err != nil {
		return err
	}
	projectServiceClient := pb.NewProjectServiceClient(conn)

	projectSpec := &pb.ProjectSpecification{
		Name:   clientConfig.Project.Name,
		Config: clientConfig.Project.Config,
	}
	registerResponse, err := projectServiceClient.RegisterProject(registerTimeoutCtx, &pb.RegisterProjectRequest{
		Project: projectSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(fmt.Sprintf("ignoring project config changes: %v", err))
			return nil
		}
		return fmt.Errorf("failed to register or update project: %w", err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to register or update project, %s", registerResponse.Message)
	}
	l.Info("project registration finished successfully")
	return nil
}
