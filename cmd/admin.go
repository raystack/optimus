package cmd

import (
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

// adminCommand requests a resource from optimus
func adminCommand(l log.Logger, pluginRepo models.PluginRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "admin",
		Short: "administration commands, should not be used by user",
	}
	cmd.AddCommand(adminBuildCommand(l))
	cmd.AddCommand(adminGetCommand(l, pluginRepo))
	return cmd
}

// adminBuildCommand builds a resource
func adminBuildCommand(l log.Logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(adminBuildInstanceCommand(l))
	return cmd
}

// adminGetCommand gets a resource
func adminGetCommand(l log.Logger, pluginRepo models.PluginRepository) *cli.Command {
	cmd := &cli.Command{
		Use: "get",
	}
	cmd.AddCommand(adminGetStatusCommand(l))
	return cmd
}
