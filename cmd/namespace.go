package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
)

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
		return err
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
