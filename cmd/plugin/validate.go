package plugin

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	yml "gopkg.in/yaml.v2"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/yaml"
)

type validateCommand struct {
	logger        log.Logger
	serverConfig  *config.ServerConfig
	path          string
	logYaml       bool
	pluginVersion string
}

// NewInstallCommand initializes plugin install command
func NewValidateCommand(serverConfig *config.ServerConfig) *cobra.Command {
	validate := &validateCommand{
		serverConfig: serverConfig,
	}
	cmd := &cobra.Command{
		Use:     "validate",
		Short:   "validate installed plugins",
		Example: "optimus plugin validate --path bq2bq.yaml --pluginversion 0.2.2 --print",
		RunE:    validate.RunE,
		PreRunE: validate.PreRunE,
	}
	cmd.Flags().StringVar(&validate.path, "path", ".plugins", "file or dir of plugins")
	cmd.Flags().BoolVar(&validate.logYaml, "print", false, "prints yaml plugin model")
	cmd.Flags().StringVar(&validate.pluginVersion, "pluginversion", "", "overrides pluginversion in yaml file")
	return cmd
}

func (v *validateCommand) PreRunE(_ *cobra.Command, _ []string) error {
	v.logger = logger.NewClientLogger(v.serverConfig.Log)
	return nil
}

func (v *validateCommand) overrideFile(plugin models.CommandLineMod, pluginPath string) error {
	if v.pluginVersion == "" {
		return nil
	}
	pluginRef := plugin.(*yaml.PluginSpec)
	pluginRef.PluginVersion = v.pluginVersion
	data, marshalErr := yml.Marshal(pluginRef)
	if marshalErr != nil {
		return marshalErr
	}
	ioerr := ioutil.WriteFile(pluginPath, data, 0)
	if ioerr == nil {
		v.logger.Info(fmt.Sprintf("Success in Overriding pluginversion in %s", pluginPath))
	}
	return ioerr
}

func (v *validateCommand) validateFile(pluginPath string) error {
	v.logger.Info("\nValidating " + pluginPath)
	if filepath.Ext(pluginPath) != ".yaml" {
		return errors.New("expecting .yaml file at " + pluginPath)
	}
	var err error
	var plugin models.CommandLineMod
	if plugin, err = yaml.NewPluginSpec(pluginPath); err == nil {
		overrideErr := v.overrideFile(plugin, pluginPath)
		if overrideErr != nil {
			return overrideErr
		}
		v.logPluginAsYaml(&plugin)
		return nil
	}
	return err
}

func (v *validateCommand) validateDir(pluginPath string) error {
	files, err := ioutil.ReadDir(pluginPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		path := filepath.Join(pluginPath, file.Name())
		if file.IsDir() {
			v.logger.Error("skipping dir : " + path)
			continue
		}
		err := v.validateFile(path)
		if err != nil {
			v.logger.Error(err.Error())
		}
	}
	v.logger.Info("validation complete !")
	return nil
}

func (v *validateCommand) logPluginAsYaml(plugin *models.CommandLineMod) {
	if !v.logYaml {
		return
	}
	yamlData, err := yml.Marshal(plugin)
	if err != nil {
		v.logger.Error(err.Error())
		return
	}
	v.logger.Info(string(yamlData))
}

func (v *validateCommand) RunE(_ *cobra.Command, _ []string) error {
	fileInfo, err := os.Stat(v.path)
	if err != nil {
		return err
	}
	fm := fileInfo.Mode()
	if fm.IsRegular() {
		if err := v.validateFile(v.path); err != nil {
			return err
		}
		v.logger.Info("validation complete !")
		return nil
	} else if fm.IsDir() {
		return v.validateDir(v.path)
	} else {
		return errors.New("invalid path")
	}
}
