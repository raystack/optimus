package cmd

import (
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

func jobCommand() *cli.Command {
	var configFilePath string
	conf := &config.ClientConfig{}
	l := initDefaultLogger()

	cmd := &cli.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		var err error

		conf, err = config.LoadClientConfig(configFilePath)
		if err != nil {
			return err
		}
		l = initClientLogger(conf.Log)

		// TODO: refactor initialize client deps
		pluginCleanFn, err := initializeClientPlugins(conf.Log.Level)
		defer pluginCleanFn()
		if err != nil {
			return err
		}

		return nil
	}

	cmd.AddCommand(jobCreateCommand(l, conf))
	cmd.AddCommand(jobAddHookCommand(l, conf))
	cmd.AddCommand(jobRenderTemplateCommand(l, conf))
	cmd.AddCommand(jobValidateCommand(l, conf))
	cmd.AddCommand(jobRunCommand(l, conf))
	cmd.AddCommand(jobRunListCommand(l, conf.Project.Name, conf.Host))
	cmd.AddCommand(jobRefreshCommand(l, conf))
	return cmd
}
