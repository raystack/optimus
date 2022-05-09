package project

import (
	"fmt"
	"path"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/config"
)

const describeTimeout = time.Minute * 15

type describeCommand struct {
	logger log.Logger
}

// NewDescribeCommand initializes command to describe a project
func NewDescribeCommand(logger log.Logger) *cobra.Command {
	describe := &describeCommand{
		logger: logger,
	}

	cmd := &cobra.Command{
		Use:     "describe",
		Short:   "Describes project configuration in the selected server",
		Example: "optimus project describe [--flag]",
		RunE:    describe.RunE,
	}
	cmd.Flags().String("dir", "", "Directory where the Optimus client config resides")
	cmd.Flags().String("host", "", "Targeted server host, by default taking from client config")
	cmd.Flags().String("project-name", "", "Targeted project name, by default taking from client config")
	return cmd
}

func (d *describeCommand) RunE(cmd *cobra.Command, args []string) error {
	dirPath, _ := cmd.Flags().GetString("dir")

	filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
	clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
	if err != nil {
		return err
	}

	d.logger.Info(fmt.Sprintf("Getting project [%s] from host [%s]", clientConfig.Project.Name, clientConfig.Host))
	project, err := d.getProject(clientConfig.Project.Name, clientConfig.Host)
	if err != nil {
		return err
	}
	marshalledProject, err := yaml.Marshal(project)
	if err != nil {
		return err
	}
	d.logger.Info("Successfully getting project!")
	d.logger.Info(fmt.Sprintf("============================\n%s", string(marshalledProject)))
	return nil
}

func (d *describeCommand) getProject(projectName, serverHost string) (config.Project, error) {
	var project config.Project
	conn, err := connectivity.NewConnectivity(serverHost, describeTimeout)
	if err != nil {
		return project, err
	}
	defer conn.Close()

	request := &pb.GetProjectRequest{
		ProjectName: projectName,
	}

	projectServiceClient := pb.NewProjectServiceClient(conn.GetConnection())
	response, err := projectServiceClient.GetProject(conn.GetContext(), request)
	if err != nil {
		return project, err
	}
	return config.Project{
		Name:   response.GetProject().Name,
		Config: response.GetProject().Config,
	}, nil
}
