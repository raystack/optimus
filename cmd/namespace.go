package cmd

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
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
	return cmd
}

func namespaceDescribeCommand() *cli.Command {
	var dirPath, serverHost, projectName, namespaceName string
	cmd := &cli.Command{
		Use:     "describe",
		Short:   "Describes namespace configuration in the selected server",
		Example: "optimus namespace describe [--flag]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initDefaultLogger()
		filePath := path.Join(dirPath, config.DefaultFilename+"."+config.DefaultFileExtension)
		clientConfig, err := config.LoadClientConfig(filePath, cmd.Flags())
		if projectName == "" {
			if err != nil {
				return err
			}
			projectName = clientConfig.Project.Name
			l.Info(fmt.Sprintf("Using project name from client config: %s", projectName))
		}
		if namespaceName == "" {
			if err != nil {
				return err
			}
			return errors.New("Namespace name is required")
		}
		if serverHost == "" {
			if err != nil {
				return err
			}
			serverHost = clientConfig.Host
			l.Info(fmt.Sprintf("Using server host from client config: %s", serverHost))
		}
		namespace, err := getNamespace(projectName, namespaceName, serverHost)
		if err != nil {
			return err
		}
		// TODO: need a refactor to make it cleaner
		stringifyNamespace := func(n config.Namespace) string {
			output := fmt.Sprintf("name: %s\n", n.Name)
			if len(n.Config) == 0 {
				output += "config: {}"
			} else {
				output += "config:\n"
				for key, value := range n.Config {
					output += fmt.Sprintf("\t%s: %s", key, value)
				}
			}
			return output
		}
		l.Info("Successfully getting namespace!")
		l.Info(fmt.Sprintf("==============================\n%s", stringifyNamespace(namespace)))
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config resides")
	cmd.Flags().StringVar(&serverHost, "server", serverHost, "Targeted server host, by default taking from client config")
	cmd.Flags().StringVar(&projectName, "project-name", projectName, "Targeted project name, by default taking from client config")
	cmd.Flags().StringVar(&namespaceName, "name", namespaceName, "Targeted namespace name, by default taking from client config")
	return cmd
}

func getNamespace(projectName, namespaceName, serverHost string) (config.Namespace, error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	requestTimeoutCtx, registerCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer registerCancel()

	var namespace config.Namespace
	conn, err := createConnection(dialTimeoutCtx, serverHost)
	if err != nil {
		return namespace, fmt.Errorf("failed creating connection to [%s]: %w", serverHost, err)
	}

	request := &pb.GetNamespaceRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
	}

	namespaceServiceClient := pb.NewNamespaceServiceClient(conn)
	response, err := namespaceServiceClient.GetNamespace(requestTimeoutCtx, request)
	if err != nil {
		return namespace, fmt.Errorf("Unable to get namespace [%s]: %w", namespaceName, err)
	}
	return config.Namespace{
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
			return registerOneNamespace(l, clientConfig, namespaceName)
		}
		l.Info("Registering all available namespaces")
		return registerAllNamespaces(l, clientConfig)
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

func getValidatedNamespaces(clientConfig *config.ClientConfig, selectedNamespaceNames []string) (validNames []string, invalidNames []string) {
	if len(selectedNamespaceNames) == 0 {
		return
	}
	for _, name := range selectedNamespaceNames {
		if _, err := clientConfig.GetNamespaceByName(name); err != nil {
			invalidNames = append(invalidNames, name)
		} else {
			validNames = append(validNames, name)
		}
	}
	return
}

func registerAllNamespaces(l log.Logger, clientConfig *config.ClientConfig) error {
	var selectedNamespaceNames []string
	for _, namespace := range clientConfig.Namespaces {
		selectedNamespaceNames = append(selectedNamespaceNames, namespace.Name)
	}
	return registerSelectedNamespaces(l, clientConfig, selectedNamespaceNames)
}

func registerSelectedNamespaces(l log.Logger, clientConfig *config.ClientConfig, selectedNamespaceNames []string) error {
	ch := make(chan error, len(selectedNamespaceNames))
	defer close(ch)

	for _, namespaceName := range selectedNamespaceNames {
		go func(name string) {
			ch <- registerOneNamespace(l, clientConfig, name)
		}(namespaceName)
	}
	var errMsg string
	for i := 0; i < len(selectedNamespaceNames); i++ {
		if err := <-ch; err != nil {
			errMsg += err.Error() + "\n"
		}
	}
	if len(errMsg) > 0 {
		return errors.New(errMsg)
	}
	return nil
}

func registerOneNamespace(l log.Logger, clientConfig *config.ClientConfig, namespaceName string) error {
	namespace, err := clientConfig.GetNamespaceByName(namespaceName)
	if err != nil {
		return err
	}

	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()
	registerTimeoutCtx, registerCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer registerCancel()

	conn, err := createConnection(dialTimeoutCtx, clientConfig.Host)
	if err != nil {
		return fmt.Errorf("failed creating connection to [%s]: %w", clientConfig.Host, err)
	}
	namespaceServiceClient := pb.NewNamespaceServiceClient(conn)

	registerResponse, err := namespaceServiceClient.RegisterProjectNamespace(registerTimeoutCtx, &pb.RegisterProjectNamespaceRequest{
		ProjectName: clientConfig.Project.Name,
		Namespace: &pb.NamespaceSpecification{
			Name:   namespaceName,
			Config: namespace.Config,
		},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(fmt.Sprintf("ignoring namespace [%s] config changes: %v", namespaceName, err))
			return nil
		}
		return fmt.Errorf("failed to register or update namespace [%s]: %w", namespaceName, err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update namespace [%s]: %s", namespaceName, registerResponse.Message)
	}
	l.Info(fmt.Sprintf("namespace [%s] registration finished successfully", namespaceName))
	return nil
}

func validateNamespaces(datastoreSpecFs map[string]map[string]afero.Fs, selectedNamespaceNames []string) error {
	var unknownNamespaceNames []string
	for _, namespaceName := range selectedNamespaceNames {
		if datastoreSpecFs[namespaceName] == nil {
			unknownNamespaceNames = append(unknownNamespaceNames, namespaceName)
		}
	}
	if len(unknownNamespaceNames) > 0 {
		return fmt.Errorf("namespaces [%s] are not found", strings.Join(unknownNamespaceNames, ", "))
	}
	return nil
}
