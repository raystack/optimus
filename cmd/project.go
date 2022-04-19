package cmd

import (
	"fmt"
	"path"

	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

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
	cmd.AddCommand(projectDescribeCommand())
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
		filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if err != nil {
			return err
		}
		l := initDefaultLogger()
		l.Info(fmt.Sprintf("Registering project [%s] to server [%s]", clientConfig.Project.Name, clientConfig.Host))
		if err := registerProject(l, clientConfig.Host, clientConfig.Project); err != nil {
			return err
		}
		if withNamespaces {
			l.Info(fmt.Sprintf("Registering all namespaces from: %s", filePath))
			if err := registerSelectedNamespaces(l, clientConfig.Host, clientConfig.Project.Name, clientConfig.Namespaces...); err != nil {
				return err
			}
		}
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().BoolVar(&withNamespaces, "with-namespaces", withNamespaces, "If yes, then namespace will be registered or updated as well")
	return cmd
}

func projectDescribeCommand() *cli.Command {
	var dirPath, host, projectName string
	cmd := &cli.Command{
		Use:     "describe",
		Short:   "Describes project configuration in the selected server",
		Example: "optimus project describe [--flag]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initDefaultLogger()
		filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if err != nil {
			return err
		}

		l.Info(fmt.Sprintf("Getting project [%s] from host [%s]", clientConfig.Project.Name, clientConfig.Host))
		project, err := getProject(clientConfig.Project.Name, clientConfig.Host)
		if err != nil {
			return err
		}
		marshalledProject, err := yaml.Marshal(project)
		if err != nil {
			return err
		}
		l.Info("Successfully getting project!")
		l.Info(fmt.Sprintf("============================\n%s", string(marshalledProject)))
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().StringVar(&host, "host", host, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&projectName, "project-name", projectName, "Targeted project name, by default taking from client config")
	return cmd
}

func getProject(projectName, host string) (config.Project, error) {
	var project config.Project
	ctx, conn, closeConn, err := initClientConnection(host, deploymentTimeout)
	if err != nil {
		return project, err
	}
	defer closeConn()

	request := &pb.GetProjectRequest{
		ProjectName: projectName,
	}

	projectServiceClient := pb.NewProjectServiceClient(conn)
	response, err := projectServiceClient.GetProject(ctx, request)
	if err != nil {
		return project, err
	}
	return config.Project{
		Name:   response.GetProject().Name,
		Config: response.GetProject().Config,
	}, nil
}

func registerProject(l log.Logger, serverHost string, project config.Project) error {
	ctx, conn, closeConn, err := initClientConnection(serverHost, deploymentTimeout)
	if err != nil {
		return err
	}
	defer closeConn()

	projectServiceClient := pb.NewProjectServiceClient(conn)
	projectSpec := &pb.ProjectSpecification{
		Name:   project.Name,
		Config: project.Config,
	}
	registerResponse, err := projectServiceClient.RegisterProject(ctx, &pb.RegisterProjectRequest{
		Project: projectSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(fmt.Sprintf("Ignoring project config changes: %v", err))
			return nil
		}
		return fmt.Errorf("failed to register or update project: %w", err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to register or update project, %s", registerResponse.Message)
	}
	l.Info("Project registration finished successfully")
	return nil
}
