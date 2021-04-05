package commands

import (
	cli "github.com/spf13/cobra"
)

// adminCommand requests a resource from optimus
func adminCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "admin",
		Short: "administration commands, should not be used by user",
	}
	cmd.AddCommand(adminBuildCommand(l))
	cmd.AddCommand(adminGetCommand(l))
	return cmd
}

// adminBuildCommand builds a resource
func adminBuildCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(adminBuildInstanceCommand(l))
	return cmd
}

// adminGetCommand gets a resource
func adminGetCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use: "get",
	}
	cmd.AddCommand(adminGetStatusCommand(l))
	return cmd
}
