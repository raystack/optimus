package cmd

import (
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

// adminCommand registers internal administration commands
func adminCommand(rootCmd *cli.Command) *cli.Command {
	var (
		configFilePath string
		conf           config.ClientConfig
	)

	cmd := &cli.Command{
		Use:    "admin",
		Short:  "Internal administration commands",
		Hidden: true,
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

		return nil
	}

	cmd.AddCommand(adminBuildCommand(&conf))
	return cmd
}

// adminBuildCommand builds a run instance
func adminBuildCommand(conf *config.ClientConfig) *cli.Command {
	cmd := &cli.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(adminBuildInstanceCommand(conf))
	return cmd
}
