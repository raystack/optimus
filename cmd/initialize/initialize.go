package initialize

import (
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/utils"
)

type initializeCommand struct {
	logger     log.Logger
	initSurvey *survey.InititalizeSurvey

	dirPath string
}

// NewInitializeCommand initializes command to interactively initialize client config
func NewInitializeCommand() *cobra.Command {
	logger := logger.NewDefaultLogger()
	initialize := &initializeCommand{
		logger:     logger,
		initSurvey: survey.NewInitializeSurvey(logger),
	}
	cmd := &cobra.Command{
		Use:     "init",
		Short:   "Interactively initialize Optimus client config",
		Example: "optimus init [--dir]",
		RunE:    initialize.RunE,
	}
	cmd.Flags().StringVar(&initialize.dirPath, "dir", initialize.dirPath, "Directory where the Optimus client config will be stored")
	return cmd
}

func (i *initializeCommand) RunE(_ *cobra.Command, _ []string) error {
	filePath := i.getClientConfigPath()

	pathOccupied, err := utils.IsPathOccupied(filePath)
	if err != nil {
		return err
	}
	if pathOccupied {
		confirmMessage := fmt.Sprintf("Path [%s] already exist, do you want to replace?", filePath)
		confirmHelp := "If yes, then targeted file or directory will be replaced"
		confirmedToReplace, err := i.initSurvey.AskToConfirm(confirmMessage, confirmHelp, false)
		if err != nil {
			return err
		}
		if !confirmedToReplace {
			i.logger.Info("Confirmed NOT to replace, exiting process")
			return nil
		}
	}

	clientConfig, err := i.initSurvey.AskInitClientConfig(i.dirPath)
	if err != nil {
		return err
	}

	if err := i.initClientConfig(clientConfig); err != nil {
		return err
	}
	i.logger.Info("Client config is initialized successfully")
	i.logger.Info(fmt.Sprintf("If you want to modify, go to [%s]", filePath))
	return nil
}

func (i *initializeCommand) initClientConfig(clientConfig *config.ClientConfig) error {
	if err := i.setupDirPathForClientConfig(clientConfig); err != nil {
		return err
	}
	marshalledClientConfig, err := yaml.Marshal(clientConfig)
	if err != nil {
		return err
	}
	filePath := i.getClientConfigPath()
	filePermission := 0o660
	return os.WriteFile(filePath, marshalledClientConfig, fs.FileMode(filePermission))
}

func (i *initializeCommand) setupDirPathForClientConfig(clientConfig *config.ClientConfig) error {
	directoryPermission := 0o750
	for _, namespace := range clientConfig.Namespaces {
		namespaceDirPath := path.Join(i.dirPath, namespace.Name)
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

func (i *initializeCommand) getClientConfigPath() string {
	fileName := fmt.Sprintf("%s.%s", config.DefaultFilename, config.DefaultFileExtension)
	return path.Join(i.dirPath, fileName)
}
