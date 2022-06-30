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

	dirPath     string
	host        string
	projectName string
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
	cmd.Flags().StringVar(&list.dirPath, "dir", list.dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().StringVar(&list.host, "host", list.host, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&list.projectName, "project-name", list.projectName, "Targeted project name, by default taking from client config")
	return cmd
}

func (l *listCommand) RunE(cmd *cobra.Command, _ []string) error {
	filePath := path.Join(l.dirPath, config.DefaultFilename)
	clientConfig, err := config.LoadClientConfig(filePath)
	if err != nil {
		return err
	}

	l.logger.Info(fmt.Sprintf("Getting all namespaces for project [%s] from [%s]", clientConfig.Project.Name, clientConfig.Host))
	namespacesFromServer, err := l.listNamespacesFromServer(clientConfig.Host, clientConfig.Project.Name)
	if err != nil {
		return err
	}
	var namespacesFromLocal []*config.Namespace
	if l.projectName != "" {
		namespacesFromLocal = clientConfig.Namespaces
	}
	result := l.stringifyNamespaces(namespacesFromLocal, namespacesFromServer)
	l.logger.Info("Successfully getting namespace!")
	l.logger.Info(fmt.Sprintf("==============================\n%s", result))
	return nil
}

func (*listCommand) listNamespacesFromServer(serverHost, projectName string) ([]*config.Namespace, error) {
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

func (*listCommand) stringifyNamespaces(namespacesFromLocal, namespacesFromServer []*config.Namespace) string {
	namespaceNames := make(map[string]bool)
	registeredInServer := make(map[string]bool)
	for _, namespace := range namespacesFromServer {
		namespaceNames[namespace.Name] = true
		registeredInServer[namespace.Name] = true
	}
	registeredInLocal := make(map[string]bool)
	for _, namespace := range namespacesFromLocal {
		namespaceNames[namespace.Name] = true
		registeredInLocal[namespace.Name] = true
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
