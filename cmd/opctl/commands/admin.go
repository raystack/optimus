package commands

import (
	cli "github.com/spf13/cobra"
	"github.com/odpf/optimus/models"
)

// adminCommand requests a resource from optimus
func adminCommand(l logger, taskRepo models.TaskPluginRepository, hookRepo models.HookRepo) *cli.Command {
	cmd := &cli.Command{
		Use:   "admin",
		Short: "administration commands, should not be used by user",
	}
	cmd.AddCommand(adminBuildCommand(l))
	cmd.AddCommand(adminGetCommand(l, taskRepo, hookRepo))
	return cmd
}

// adminBuildCommand builds a resource
func adminBuildCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(adminBuildInstanceCommand(l))
	cmd.AddCommand(adminBuildPluginCommand(l))
	return cmd
}

// adminGetCommand gets a resource
func adminGetCommand(l logger, taskRepo models.TaskPluginRepository, hookRepo models.HookRepo) *cli.Command {
	cmd := &cli.Command{
		Use: "get",
	}
	cmd.AddCommand(adminGetStatusCommand(l))
	cmd.AddCommand(adminGetPluginsCommand(l, taskRepo, hookRepo))
	return cmd
}
