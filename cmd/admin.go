package cmd

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

// adminCommand registers internal administration commands
func adminCommand(l log.Logger, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:    "admin",
		Short:  "Internal administration commands",
		Hidden: true,
	}
	cmd.AddCommand(adminBuildCommand(l, conf))
	return cmd
}

// adminBuildCommand builds a run instance
func adminBuildCommand(l log.Logger, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "build",
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(adminBuildInstanceCommand(l, conf))
	return cmd
}
