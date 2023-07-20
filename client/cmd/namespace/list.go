package namespace

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/internal"
	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/config"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

const listTimeout = time.Minute * 15

type listCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string
	clientConfig   *config.ClientConfig

	dirPath     string
	host        string
	projectName string
}

// NewListCommand initializes command for listing namespace
func NewListCommand() *cobra.Command {
	list := &listCommand{
		clientConfig: &config.ClientConfig{},
		logger:       logger.NewClientLogger(),
	}
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Lists namespaces from the selected server and project",
		Example: "optimus namespace list [--flag]",
		RunE:    list.RunE,
		PreRunE: list.PreRunE,
	}

	list.injectFlags(cmd)
	return cmd
}

func (l *listCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&l.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&l.dirPath, "dir", l.dirPath, "Directory where the Optimus client config resides")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&l.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&l.host, "host", "", "Optimus service endpoint url")
}

func (l *listCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	if l.dirPath != "" {
		l.configFilePath = path.Join(l.dirPath, config.DefaultFilename)
	}
	// Load config
	conf, err := internal.LoadOptionalConfig(l.configFilePath)
	if err != nil {
		return err
	}
	l.clientConfig = conf

	if l.clientConfig == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if l.projectName == "" {
		l.projectName = l.clientConfig.Project.Name
	}
	if l.host == "" {
		l.host = l.clientConfig.Host
	}
	l.connection = connection.New(l.logger, conf)

	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	l.logger.Info("Getting all namespaces for project [%s] from [%s]", l.projectName, l.host)
	namespacesFromServer, err := l.listNamespacesFromServer(l.host, l.projectName)
	if err != nil {
		return err
	}
	var namespacesFromLocal []*config.Namespace
	if l.projectName != "" && l.clientConfig != nil {
		namespacesFromLocal = l.clientConfig.Namespaces
	}
	result := l.stringifyNamespaces(namespacesFromLocal, namespacesFromServer)
	l.logger.Info("Successfully getting namespace!")
	l.logger.Info("==============================\n%s", result)
	return nil
}

func (l *listCommand) listNamespacesFromServer(serverHost, projectName string) ([]*config.Namespace, error) {
	conn, err := l.connection.Create(serverHost)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.ListProjectNamespacesRequest{
		ProjectName: projectName,
	}
	namespaceServiceClient := pb.NewNamespaceServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), listTimeout)
	defer cancelFunc()

	response, err := namespaceServiceClient.ListProjectNamespaces(ctx, request)
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
