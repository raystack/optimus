package namespace

import (
	"fmt"
	"path"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/internal"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
)

const describeTimeout = time.Minute * 15

type describeCommand struct {
	logger         log.Logger
	configFilePath string

	dirPath       string
	host          string
	projectName   string
	namespaceName string
}

// NewDescribeCommand initializes command to describe namespace
func NewDescribeCommand() *cobra.Command {
	describe := &describeCommand{}

	cmd := &cobra.Command{
		Use:     "describe",
		Short:   "Describes namespace configuration from the selected server and project",
		Example: "optimus namespace describe [--flag]",
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

	cmd.Flags().StringVar(&d.namespaceName, "name", d.namespaceName, "Targeted namespace name, by default taking from client config")
	cmd.MarkFlagRequired("name")

	// Mandatory flags if config is not set
	cmd.Flags().StringVar(&d.host, "host", d.host, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&d.projectName, "project-name", d.projectName, "Targeted project name, by default taking from client config")
}

func (d *describeCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	if d.dirPath != "" {
		d.configFilePath = path.Join(d.dirPath, config.DefaultFilename)
	}
	// Load config
	conf, err := internal.LoadOptionalConfig(d.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		d.logger = logger.NewDefaultLogger()
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	d.logger = logger.NewClientLogger(conf.Log)
	if d.projectName == "" {
		d.projectName = conf.Project.Name
	}
	if d.host == "" {
		d.host = conf.Host
	}

	return nil
}

func (d *describeCommand) RunE(_ *cobra.Command, _ []string) error {
	d.logger.Info(
		fmt.Sprintf("Getting namespace [%s] in project [%s] from [%s]",
			d.namespaceName, d.projectName, d.host,
		),
	)
	namespace, err := d.getNamespace()
	if err != nil {
		return err
	}
	result := d.stringifyNamespace(namespace)
	d.logger.Info("Successfully getting namespace!")
	d.logger.Info(fmt.Sprintf("==============================\n%s", result))
	return nil
}

func (d *describeCommand) getNamespace() (*config.Namespace, error) {
	conn, err := connectivity.NewConnectivity(d.host, describeTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.GetNamespaceRequest{
		ProjectName:   d.projectName,
		NamespaceName: d.namespaceName,
	}
	namespaceServiceClient := pb.NewNamespaceServiceClient(conn.GetConnection())
	response, err := namespaceServiceClient.GetNamespace(conn.GetContext(), request)
	if err != nil {
		return nil, fmt.Errorf("unable to get namespace [%s]: %w", d.namespaceName, err)
	}
	return &config.Namespace{
		Name:   response.GetNamespace().Name,
		Config: response.GetNamespace().Config,
	}, nil
}

func (*describeCommand) stringifyNamespace(namespace *config.Namespace) string {
	output := fmt.Sprintf("name: %s\n", namespace.Name)
	if len(namespace.Config) == 0 {
		output += "config: {}"
	} else {
		output += "config:\n"
		for key, value := range namespace.Config {
			output += fmt.Sprintf("\t%s: %s", key, value)
		}
	}
	return output
}
