package cmd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/odpf/optimus/config"
)

func initCommand() *cli.Command {
	var dirPath string
	cmd := &cli.Command{
		Use:     "init",
		Short:   "Interactively initialize Optimus client config",
		Example: "optimus init [--dir]",
	}
	cmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initDefaultLogger()

		filePath := getClientConfigPath(dirPath)
		pathOccupied, err := isPathOccupied(filePath)
		if err != nil {
			return err
		}
		if pathOccupied {
			confirmMessage := fmt.Sprintf("Path [%s] already exist, do you want to replace?", filePath)
			confirmHelp := "If yes, then targeted file or directory will be replaced"
			confirmedToReplace, err := askToConfirm(confirmMessage, confirmHelp, false)
			if err != nil {
				return err
			}
			if !confirmedToReplace {
				l.Info("Confirmed NOT to replace, exiting process")
				return nil
			}
		}
		clientConfig, err := askInitClientConfig(l, dirPath)
		if err != nil {
			return err
		}
		if err := initClientConfig(dirPath, clientConfig); err != nil {
			return err
		}
		l.Info("Client config is initialized successfully")
		l.Info(fmt.Sprintf("If you want to modify, go to [%s]", filePath))
		return nil
	}
	cmd.Flags().StringVar(&dirPath, "dir", dirPath, "Directory where the Optimus client config will be stored")

	return cmd
}

func initClientConfig(dirPath string, clientConfig *config.ClientConfig) error {
	if err := setupDirPathForClientConfig(dirPath, clientConfig); err != nil {
		return err
	}
	marshalledClientConfig, err := yaml.Marshal(clientConfig)
	if err != nil {
		return err
	}
	filePath := getClientConfigPath(dirPath)
	filePermission := 0o660
	return os.WriteFile(filePath, marshalledClientConfig, fs.FileMode(filePermission))
}

func setupDirPathForClientConfig(dirPath string, clientConfig *config.ClientConfig) error {
	directoryPermission := 0o750
	for _, namespace := range clientConfig.Namespaces {
		namespaceDirPath := path.Join(dirPath, namespace.Name)
		namespaceDatastoreDirPath := path.Join(namespaceDirPath, "resources")
		namespaceJobDirPath := path.Join(namespaceDirPath, "jobs")
		paths := []string{namespaceDirPath, namespaceDatastoreDirPath, namespaceJobDirPath}
		for _, p := range paths {
			if err := os.RemoveAll(p); err != nil {
				return fmt.Errorf("error deleting [%s] for namespace [%s]: %w",
					p, namespace.Name, err,
				)
			}
			if err := os.Mkdir(p, fs.FileMode(directoryPermission)); err != nil {
				return fmt.Errorf("error creating [%s] for namespace [%s]: %w",
					p, namespace.Name, err,
				)
			}
		}
	}
	return nil
}

func askInitClientConfig(l log.Logger, dirPath string) (*config.ClientConfig, error) {
	output := &config.ClientConfig{}
	host, err := askInitHost(l)
	if err != nil {
		return nil, err
	}
	project, err := askInitProject(l)
	if err != nil {
		return nil, err
	}
	namespaces, err := askInitNamespaces(l, dirPath)
	if err != nil {
		return nil, err
	}
	output.Version = config.DefaultVersion
	output.Host = host
	output.Project = project
	output.Namespaces = namespaces
	output.Log = config.LogConfig{
		Level: config.LogLevelInfo,
	}
	return output, nil
}

func askInitNamespaces(l log.Logger, dirPath string) ([]*config.Namespace, error) {
	var output []*config.Namespace
	for {
		name, err := askInitNamespaceName(l, dirPath)
		if err != nil {
			return nil, err
		}
		datastoreType, err := askInitNamespaceDatastoreType()
		if err != nil {
			return nil, err
		}
		namespace := &config.Namespace{
			Name: name,
			Datastore: []config.Datastore{
				{
					Type:   datastoreType,
					Path:   path.Join(name, "resources"),
					Backup: make(map[string]string),
				},
			},
			Job: config.Job{
				Path: path.Join(name, "jobs"),
			},
		}
		output = append(output, namespace)

		confirmMessage := "Do you want to add another namespace?"
		confirmHelp := "If yes, then you will be prompted to create another namespace"
		confirmedToAddMore, err := askToConfirm(confirmMessage, confirmHelp, false)
		if err != nil {
			return nil, err
		}
		if !confirmedToAddMore {
			break
		}
		l.Info("Adding more namespaces")
	}
	return output, nil
}

func askInitNamespaceDatastoreType() (string, error) {
	prompt := &survey.Select{
		Message: "What is the type of data store for this namespace?",
		Options: []string{
			"bigquery",
		},
		Default: "bigquery",
	}
	var dataStoreType string
	if err := survey.AskOne(prompt, &dataStoreType); err != nil {
		return dataStoreType, err
	}
	return dataStoreType, nil
}

func askInitNamespaceName(l log.Logger, dirPath string) (string, error) {
	for {
		prompt := &survey.Input{
			Message: "What is the namespace name?",
		}
		var name string
		if err := survey.AskOne(prompt, &name); err != nil {
			return name, err
		}
		if name == "" {
			l.Warn("Namespace name is empty, let's try again")
			continue
		}
		namespaceDirPath := path.Join(dirPath, name)
		pathOccupied, err := isPathOccupied(namespaceDirPath)
		if err != nil {
			return name, err
		}
		if !pathOccupied {
			return name, nil
		}
		confirmMessage := fmt.Sprintf("Directory [%s] for namespace [%s] is occupied, replace?",
			namespaceDirPath, name,
		)
		confirmHelp := fmt.Sprintf("If yes, then [%s] will be replaced for namespace [%s]",
			namespaceDirPath, name,
		)
		confirmedToReplace, err := askToConfirm(confirmMessage, confirmHelp, false)
		if err != nil {
			return name, err
		}
		if confirmedToReplace {
			l.Info(fmt.Sprintf("Confirmed to replace [%s] for namespace [%s]", namespaceDirPath, name))
			return name, nil
		}
		l.Info(fmt.Sprintf("Confirmed NOT to replace [%s], let's initiate another namespace", namespaceDirPath))
	}
}

func askInitProject(l log.Logger) (project config.Project, err error) {
	for {
		prompt := &survey.Input{
			Message: "What is the Optimus project name?",
		}
		var projectName string
		if err = survey.AskOne(prompt, &projectName); err != nil {
			return
		}
		if projectName == "" {
			l.Warn("Project name is empty, let's try again")
			continue
		}
		project.Name = projectName
		project.Config = make(map[string]string)
		return
	}
}

func askInitHost(l log.Logger) (host string, err error) {
	for {
		prompt := &survey.Input{
			Message: "What is the Optimus service host?",
			Help:    "Example - localhost:9100",
		}
		if err = survey.AskOne(prompt, &host); err != nil {
			return
		}
		if host != "" {
			return
		}
		l.Warn("Host name is empty, let's try again")
	}
}

func askToConfirm(message, help string, defaultValue bool) (bool, error) {
	prompt := &survey.Confirm{
		Message: message,
		Help:    help,
		Default: defaultValue,
	}
	var response bool
	if err := survey.AskOne(prompt, &response); err != nil {
		return defaultValue, err
	}
	return response, nil
}

func isPathOccupied(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func getClientConfigPath(dirPath string) string {
	fileName := fmt.Sprintf("%s.%s", config.DefaultFilename, config.DefaultFileExtension)
	return path.Join(dirPath, fileName)
}
