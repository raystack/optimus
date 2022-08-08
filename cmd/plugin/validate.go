package plugin

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/plugin/yaml"
)

type validateCommand struct {
	logger       log.Logger
	serverConfig *config.ServerConfig
	path         string
}

// NewInstallCommand initializes plugin install command
func NewValidateCommand(serverConfig *config.ServerConfig) *cobra.Command {
	validate := &validateCommand{
		serverConfig: serverConfig,
	}
	cmd := &cobra.Command{
		Use:     "validate",
		Short:   "validate installed plugins",
		Example: "optimus plugin validate -path bq2bq.yaml",
		RunE:    validate.RunE,
		PreRunE: validate.PreRunE,
	}
	cmd.Flags().StringVar(&validate.path, "path", ".plugins", "validate plugin, given file or folder")
	return cmd
}

func (i *validateCommand) PreRunE(_ *cobra.Command, _ []string) error {
	i.logger = logger.NewClientLogger(i.serverConfig.Log)
	return nil
}

func validateFile(pluginPath string, logger log.Logger) error {
	logger.Info("validatig " + pluginPath)
	if filepath.Ext(pluginPath) != ".yaml" {
		return errors.New("expecting .yaml file at " + pluginPath)
	}
	_, err := yaml.NewYamlPlugin(pluginPath)
	return err
}

func validateDir(pluginPath string, logger log.Logger) error {
	files, err := ioutil.ReadDir(pluginPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		path := filepath.Join(pluginPath, file.Name())
		if file.IsDir() {
			logger.Error("skipping dir : " + path)
			continue
		}
		err := validateFile(path, logger)
		if err != nil {
			logger.Error(err.Error())
		}
	}
	logger.Info("validation complete !")
	return nil
}

func (i *validateCommand) RunE(_ *cobra.Command, _ []string) error {
	fileInfo, err := os.Stat(i.path)
	if err != nil {
		return err
	}
	fm := fileInfo.Mode()
	if fm.IsRegular() {
		err := validateFile(i.path, i.logger)
		if err == nil {
			i.logger.Info("validation complete !")
		}
		return err
	} else if fm.IsDir() {
		return validateDir(i.path, i.logger)
	} else {
		return fmt.Errorf("invalid path")
	}
}
