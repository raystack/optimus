package cmd

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func jobCommand(pluginRepo models.PluginRepository) *cli.Command {
	var configFilePath string
	var conf = &config.ClientConfig{}
	var l log.Logger = initLogger(plainLoggerType, conf.Log)

	cmd := &cli.Command{

		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		var err error

		conf, err = config.LoadClientConfig(configFilePath)
		if err != nil {
			return err
		}
		l = initLogger(plainLoggerType, conf.Log)

		return nil
	}

	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.AddCommand(jobCreateCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobAddHookCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobRenderTemplateCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobValidateCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobRunCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobRunListCommand(l, conf.Project.Name, conf.Host))
	return cmd
}
