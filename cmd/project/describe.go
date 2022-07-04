package project

import (
	"errors"
	"fmt"
	"path"
	"time"

	saltConfig "github.com/odpf/salt/config"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
)

const describeTimeout = time.Minute * 15

type describeCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	dirPath     string
	host        string
	projectName string
}

// NewDescribeCommand initializes command to describe a project
func NewDescribeCommand() *cobra.Command {
	describe := &describeCommand{
		clientConfig: &config.ClientConfig{},
	}

	cmd := &cobra.Command{
		Use:     "describe",
		Short:   "Describes project configuration in the selected server",
		Example: "optimus project describe [--flag]",
		RunE:    describe.RunE,
		PreRunE: describe.PreRunE,
	}

	describe.injectFlags(cmd)
	return cmd
}

func (d *describeCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&d.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&d.dirPath, "dir", d.dirPath, "Directory where the Optimus client config resides")

	// Mandatory flags if config is not set
	cmd.Flags().StringVar(&d.host, "host", d.host, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&d.projectName, "project-name", d.projectName, "Targeted project name, by default taking from client config")
}

func (d *describeCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	if d.dirPath != "" {
		d.configFilePath = path.Join(d.dirPath, config.DefaultFilename)
	}
	// Load config
	if err := d.loadConfig(); err != nil {
		return err
	}

	if d.clientConfig == nil {
		d.logger = logger.NewDefaultLogger()
		cmd.MarkFlagRequired("project-name")
		cmd.MarkFlagRequired("host")
		return nil
	}

	d.logger = logger.NewClientLogger(d.clientConfig.Log)
	if d.projectName == "" {
		d.projectName = d.clientConfig.Project.Name
	}
	if d.host == "" {
		d.host = d.clientConfig.Host
	}

	return nil
}

func (d *describeCommand) RunE(cmd *cobra.Command, _ []string) error {
	d.logger.Info(fmt.Sprintf("Getting project [%s] from host [%s]", d.projectName, d.host))
	project, err := d.getProject()
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

func (d *describeCommand) getProject() (config.Project, error) {
	var project config.Project
	conn, err := connectivity.NewConnectivity(d.host, describeTimeout)
	if err != nil {
		return project, err
	}
	defer conn.Close()

	request := &pb.GetProjectRequest{
		ProjectName: d.projectName,
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

func (d *describeCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(d.configFilePath)
	if err != nil {
		if errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
			d.clientConfig = nil
			return nil
		}
		return err
	}
	*d.clientConfig = *c
	return nil
}
