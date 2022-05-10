package cmd

import (
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

func jobCommand(rootCmd *cli.Command) *cli.Command {
	var (
		configFilePath string
		conf           config.ClientConfig
		pluginCleanFn  func()
	)

	cmd := &cli.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		rootCmd.PersistentPreRun(cmd, args)

		// TODO: find a way to load the config in one place
		c, err := config.LoadClientConfig(configFilePath, cmd.Flags())
		if err != nil {
			return err
		}

		conf = *c

		// TODO: refactor initialize client deps
		pluginCleanFn, err = initializeClientPlugins(conf.Log.Level)
		return err
	}

	cmd.PersistentPostRunE = func(cmd *cli.Command, args []string) error {
		pluginCleanFn()
		return nil
	}

	cmd.AddCommand(jobCreateCommand(&conf))
	cmd.AddCommand(jobAddHookCommand(&conf))
	cmd.AddCommand(jobRenderTemplateCommand(&conf))
	cmd.AddCommand(jobValidateCommand(&conf))
	cmd.AddCommand(jobRunCommand(&conf))
	cmd.AddCommand(jobRunListCommand(&conf))
	cmd.AddCommand(jobRefreshCommand(&conf))
	return cmd
}
