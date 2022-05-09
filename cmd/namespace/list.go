package namespace

import (
	"bytes"
	"fmt"
	"path"
	"time"

	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/config"
)

const listTimeout = time.Minute * 15

type listCommand struct {
	logger log.Logger
}

// NewListCommand initializes command for listing namespace
func NewListCommand(logger log.Logger) *cobra.Command {
	list := &listCommand{
		logger: logger,
	}
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Lists namespaces from the selected server and project",
		Example: "optimus namespace list [--flag]",
		RunE:    list.RunE,
	}
	cmd.Flags().String("dir", "", "Directory where the Optimus client config resides")
	cmd.Flags().String("host", "", "Targeted server host, by default taking from client config")
	cmd.Flags().String("project-name", "", "Targeted project name, by default taking from client config")
	return cmd
}

func (l *listCommand) RunE(cmd *cobra.Command, args []string) error {
	dirPath, _ := cmd.Flags().GetString("dir")
	projectName, _ := cmd.Flags().GetString("project-name")

	filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
	clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
	if err != nil {
		return err
	}

	l.logger.Info(fmt.Sprintf("Getting all namespaces for project [%s] from [%s]", clientConfig.Project.Name, clientConfig.Host))
	namespacesFromServer, err := l.listNamespacesFromServer(clientConfig.Project.Name, clientConfig.Host)
	if err != nil {
		return err
	}
	var namespacesFromLocal []*config.Namespace
	if clientConfig != nil && clientConfig.Project.Name == projectName {
		namespacesFromLocal = clientConfig.Namespaces
	}
	result := l.stringifyNamespaces(namespacesFromLocal, namespacesFromServer)
	l.logger.Info("Successfully getting namespace!")
	l.logger.Info(fmt.Sprintf("==============================\n%s", result))
	return nil
}

func (l *listCommand) listNamespacesFromServer(serverHost, projectName string) ([]*config.Namespace, error) {
	conn, err := connectivity.NewConnectivity(serverHost, listTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.ListProjectNamespacesRequest{
		ProjectName: projectName,
	}
	namespaceServiceClient := pb.NewNamespaceServiceClient(conn.GetConnection())
	response, err := namespaceServiceClient.ListProjectNamespaces(conn.GetContext(), request)
	if err != nil {
		return nil, fmt.Errorf("unable to list namespace for project [%s]: %w", projectName, err)
	}
	output := make([]*config.Namespace, len(response.Namespaces))
	for i, n := range response.Namespaces {
		output[i] = &config.Namespace{
			Name:   n.GetName(),
			Config: n.GetConfig(),
		}
	}
	return output, nil
}

func (l *listCommand) stringifyNamespaces(namespacesFromLocal, namespacesFromServer []*config.Namespace) string {
	namespaceNames := make(map[string]bool)
	registeredInServer := make(map[string]bool)
	for _, namespace := range namespacesFromServer {
		registeredInServer[namespace.Name] = true
		namespaceNames[namespace.Name] = true
	}
	registeredInLocal := make(map[string]bool)
	for _, namespace := range namespacesFromLocal {
		registeredInLocal[namespace.Name] = true
		namespaceNames[namespace.Name] = true
	}

	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetHeader([]string{"namespace name", "available in local", "registered in server"})
	for key := range namespaceNames {
		record := []string{
			key,
			fmt.Sprintf("%t", registeredInLocal[key]),
			fmt.Sprintf("%t", registeredInServer[key]),
		}
		table.Append(record)
	}
	table.Render()
	return buff.String()
}
