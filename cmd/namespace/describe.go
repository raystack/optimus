package namespace

import (
	"fmt"
	"path"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/config"
)

const describeTimeout = time.Minute * 15

type describeCommand struct {
	logger log.Logger
}

// NewDescribeCommand initializes command to describe namespace
func NewDescribeCommand(logger log.Logger) *cobra.Command {
	describe := &describeCommand{
		logger: logger,
	}

	cmd := &cobra.Command{
		Use:     "describe",
		Short:   "Describes namespace configuration from the selected server and project",
		Example: "optimus namespace describe [--flag]",
		RunE:    describe.RunE,
	}
	cmd.Flags().String("dir", "", "Directory where the Optimus client config resides")
	cmd.Flags().String("host", "", "Targeted server host, by default taking from client config")
	cmd.Flags().String("project-name", "", "Targeted project name, by default taking from client config")
	cmd.Flags().String("name", "", "Targeted namespace name, by default taking from client config")
	cmd.MarkFlagRequired("name")
	return cmd
}

func (d *describeCommand) RunE(cmd *cobra.Command, args []string) error {
	dirPath, _ := cmd.Flags().GetString("dir")
	namespaceName, _ := cmd.Flags().GetString("name")

	filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
	clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
	if err != nil {
		return err
	}

	d.logger.Info(fmt.Sprintf("Getting namespace [%s] in project [%s] from [%s]", namespaceName, clientConfig.Project.Name, clientConfig.Host))
	namespace, err := d.getNamespace(clientConfig.Project.Name, namespaceName, clientConfig.Host)
	if err != nil {
		return err
	}
	result := d.stringifyNamespace(namespace)
	d.logger.Info("Successfully getting namespace!")
	d.logger.Info(fmt.Sprintf("==============================\n%s", result))
	return nil
}

func (d *describeCommand) getNamespace(serverHost, projectName, namespaceName string) (*config.Namespace, error) {
	conn, err := connectivity.NewConnectivity(serverHost, describeTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.GetNamespaceRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
	}
	namespaceServiceClient := pb.NewNamespaceServiceClient(conn.GetConnection())
	response, err := namespaceServiceClient.GetNamespace(conn.GetContext(), request)
	if err != nil {
		return nil, fmt.Errorf("unable to get namespace [%s]: %w", namespaceName, err)
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
