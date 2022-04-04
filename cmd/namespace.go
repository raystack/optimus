package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
)

func namespaceCommand() *cli.Command {
	cmd := &cli.Command{
		Use:     "namespace",
		Short:   "Commands that will let the user to operate on namespace",
		Example: "optimus namespace [sub-command]",
	}
	cmd.AddCommand(namespaceRegisterCommand())
	cmd.AddCommand(namespaceDescribeCommand())
	cmd.AddCommand(namespaceListCommand())
	return cmd
}

func namespaceListCommand() *cli.Command {
	var dirPath, serverHost, projectName string
	cmd := &cli.Command{
		Use:     "list",
		Short:   "Lists namespaces from the selected server and project",
		Example: "optimus namespace list [--flag]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initDefaultLogger()
		filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if projectName == "" {
			if dirPath == "" {
				l.Info(fmt.Sprintf("Loading project name from client config in: %s", filePath))
			}
			if err != nil {
				return err
			}
			projectName = clientConfig.Project.Name
			l.Info(fmt.Sprintf("Using project name from client config: %s", projectName))
		}
		if serverHost == "" {
			if dirPath == "" {
				l.Info(fmt.Sprintf("Loading service host from client config in: %s", filePath))
			}
			if err != nil {
				return err
			}
			serverHost = clientConfig.Host
			l.Info(fmt.Sprintf("Using server host from client config: %s", serverHost))
		}

		l.Info(fmt.Sprintf("Getting all namespaces for project [%s] from [%s]", projectName, serverHost))
		namespacesFromServer, err := listNamespacesFromServer(projectName, serverHost)
		if err != nil {
			return err
		}
		var namespacesFromLocal []*config.Namespace
		if clientConfig != nil && clientConfig.Project.Name == projectName {
			namespacesFromLocal = clientConfig.Namespaces
		}
		result := stringifyNamespacesForNamespaceList(namespacesFromLocal, namespacesFromServer)
		l.Info("Successfully getting namespace!")
		l.Info(fmt.Sprintf("==============================\n%s", result))
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().StringVar(&serverHost, "server", serverHost, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&projectName, "project-name", projectName, "Targeted project name, by default taking from client config")
	return cmd
}

func listNamespacesFromServer(projectName, serverHost string) ([]*config.Namespace, error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	requestTimeoutCtx, registerCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer registerCancel()

	conn, err := createConnection(dialTimeoutCtx, serverHost)
	if err != nil {
		return nil, fmt.Errorf("failed creating connection to [%s]: %w", serverHost, err)
	}
	defer conn.Close()

	request := &pb.ListProjectNamespacesRequest{
		ProjectName: projectName,
	}

	namespaceServiceClient := pb.NewNamespaceServiceClient(conn)
	response, err := namespaceServiceClient.ListProjectNamespaces(requestTimeoutCtx, request)
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

func stringifyNamespacesForNamespaceList(namespacesFromLocal, namespacesFromServer []*config.Namespace) string {
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

func namespaceDescribeCommand() *cli.Command {
	var dirPath, serverHost, projectName, namespaceName string
	cmd := &cli.Command{
		Use:     "describe",
		Short:   "Describes namespace configuration from the selected server and project",
		Example: "optimus namespace describe [--flag]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initDefaultLogger()
		filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if projectName == "" {
			if dirPath == "" {
				l.Info(fmt.Sprintf("Loading project name from client config in: %s", filePath))
			}
			if err != nil {
				return err
			}
			projectName = clientConfig.Project.Name
			l.Info(fmt.Sprintf("Using project name from client config: %s", projectName))
		}
		if namespaceName == "" {
			return errors.New("namespace name is required")
		}
		if serverHost == "" {
			if dirPath == "" {
				l.Info(fmt.Sprintf("Loading service host from client config in: %s", filePath))
			}
			if err != nil {
				return err
			}
			serverHost = clientConfig.Host
			l.Info(fmt.Sprintf("Using server host from client config: %s", serverHost))
		}

		l.Info(fmt.Sprintf("Getting namespace [%s] in project [%s] from [%s]", namespaceName, projectName, serverHost))
		namespace, err := getNamespace(projectName, namespaceName, serverHost)
		if err != nil {
			return err
		}
		result := stringifyNamespaceForNamespaceDescribe(namespace)
		l.Info("Successfully getting namespace!")
		l.Info(fmt.Sprintf("==============================\n%s", result))
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().StringVar(&serverHost, "server", serverHost, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&projectName, "project-name", projectName, "Targeted project name, by default taking from client config")
	cmd.Flags().StringVar(&namespaceName, "name", namespaceName, "Targeted namespace name, by default taking from client config")
	return cmd
}

func stringifyNamespaceForNamespaceDescribe(namespace *config.Namespace) string {
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

func getNamespace(projectName, namespaceName, serverHost string) (*config.Namespace, error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	requestTimeoutCtx, registerCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer registerCancel()

	conn, err := createConnection(dialTimeoutCtx, serverHost)
	if err != nil {
		return nil, fmt.Errorf("failed creating connection to [%s]: %w", serverHost, err)
	}
	defer conn.Close()

	request := &pb.GetNamespaceRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
	}

	namespaceServiceClient := pb.NewNamespaceServiceClient(conn)
	response, err := namespaceServiceClient.GetNamespace(requestTimeoutCtx, request)
	if err != nil {
		return nil, fmt.Errorf("unable to get namespace [%s]: %w", namespaceName, err)
	}
	return &config.Namespace{
		Name:   response.GetNamespace().Name,
		Config: response.GetNamespace().Config,
	}, nil
}

func namespaceRegisterCommand() *cli.Command {
	var dirPath, namespaceName string
	cmd := &cli.Command{
		Use:     "register",
		Short:   "Register namespace if it does not exist and update if it does",
		Example: "optimus namespace register [--flag]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if err != nil {
			return err
		}
		l := initDefaultLogger()
		if namespaceName != "" {
			l.Info(fmt.Sprintf("Registering namespace [%s]", namespaceName))
			namespace, err := clientConfig.GetNamespaceByName(namespaceName)
			if err != nil {
				return err
			}
			return registerNamespace(l, clientConfig.Host, clientConfig.Project.Name, namespace)
		}
		l.Info("Registering all available namespaces from client config")
		return registerSelectedNamespaces(l, clientConfig.Host, clientConfig.Project.Name, clientConfig.Namespaces...)
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().StringVar(&namespaceName, "name", namespaceName, "If set, then only that namespace will be registered")
	return cmd
}

func askToSelectNamespace(l log.Logger, conf *config.ClientConfig) (*config.Namespace, error) {
	options := make([]string, len(conf.Namespaces))
	if len(conf.Namespaces) == 0 {
		return nil, errors.New("no namespace found in config file")
	}
	for i, namespace := range conf.Namespaces {
		options[i] = namespace.Name
	}
	prompt := &survey.Select{
		Message: "Please choose the namespace:",
		Options: options,
	}
	for {
		var response string
		if err := survey.AskOne(prompt, &response); err != nil {
			return nil, err
		}
		if response == "" {
			l.Error("Namespace name cannot be empty")
			continue
		}
		namespace, err := conf.GetNamespaceByName(response)
		if err != nil {
			l.Error(err.Error())
			continue
		}
		return namespace, nil
	}
}

func registerSelectedNamespaces(l log.Logger, serverHost, projectName string, selectedNamespaces ...*config.Namespace) error {
	ch := make(chan error, len(selectedNamespaces))
	defer close(ch)

	for _, namespace := range selectedNamespaces {
		go func(namespace *config.Namespace) {
			ch <- registerNamespace(l, serverHost, projectName, namespace)
		}(namespace)
	}
	var errMsg string
	for i := 0; i < len(selectedNamespaces); i++ {
		if err := <-ch; err != nil {
			errMsg += err.Error() + "\n"
		}
	}
	if len(errMsg) > 0 {
		return errors.New(errMsg)
	}
	return nil
}

func registerNamespace(l log.Logger, serverHost, projectName string, namespace *config.Namespace) error {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	registerTimeoutCtx, registerCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer registerCancel()

	conn, err := createConnection(dialTimeoutCtx, serverHost)
	if err != nil {
		return fmt.Errorf("failed creating connection to [%s]: %w", serverHost, err)
	}
	namespaceServiceClient := pb.NewNamespaceServiceClient(conn)
	defer conn.Close()

	registerResponse, err := namespaceServiceClient.RegisterProjectNamespace(registerTimeoutCtx, &pb.RegisterProjectNamespaceRequest{
		ProjectName: projectName,
		Namespace: &pb.NamespaceSpecification{
			Name:   namespace.Name,
			Config: namespace.Config,
		},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(fmt.Sprintf("Ignoring namespace [%s] config changes: %v", namespace.Name, err))
			return nil
		}
		return fmt.Errorf("failed to register or update namespace [%s]: %w", namespace.Name, err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update namespace [%s]: %s", namespace.Name, registerResponse.Message)
	}
	l.Info(fmt.Sprintf("Namespace [%s] registration finished successfully", namespace.Name))
	return nil
}
