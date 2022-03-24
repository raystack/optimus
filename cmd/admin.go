package cmd

import (
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

// adminCommand registers internal administration commands
func adminCommand() *cli.Command {
	var configFilePath string
	var conf = &config.ClientConfig{}
	var l log.Logger = initLogger(plainLoggerType, conf.Log)

	cmd := &cli.Command{
		Use:    "admin",
		Short:  "Internal administration commands",
		Hidden: true,
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

	cmd.AddCommand(adminBuildCommand(l, conf))
	return cmd
}

// adminBuildCommand builds a run instance
func adminBuildCommand(l log.Logger, conf *config.ClientConfig) *cli.Command {
	cmd := &cli.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(adminBuildInstanceCommand(l, conf))
	return cmd
}
