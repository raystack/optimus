package plugin

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/internal/logger"
	"github.com/odpf/optimus/plugin/yaml"
)

func NewValidateCommand() *cobra.Command {
	validate := &validateCommand{}
	cmd := &cobra.Command{
		Use:     "validate",
		Short:   "validate yaml plugins",
		Example: "optimus plugin validate --path bq2bq.yaml",
		RunE:    validate.RunE,
		PreRunE: validate.PreRunE,
	}
	cmd.Flags().StringVar(&validate.path, "path", ".plugins", "file or dir of plugins")
	return cmd
}

type validateCommand struct {
	logger log.Logger
	path   string
}

func (v *validateCommand) PreRunE(_ *cobra.Command, _ []string) error {
	v.logger = logger.NewDefaultLogger()
	return nil
}

func (v *validateCommand) validateFile(pluginPath string) error {
	v.logger.Info("\nValidating " + pluginPath)
	if filepath.Ext(pluginPath) != ".yaml" {
		return errors.New("expecting .yaml file at " + pluginPath)
	}
	_, err := yaml.NewPluginSpec(pluginPath)
	if err != nil {
		return err
	}
	return nil
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
