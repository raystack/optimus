package cmd

import (
	"github.com/odpf/optimus/cmd/server"
	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func serveCommand(l log.Logger, conf config.Provider) *cli.Command {
	c := &cli.Command{
		Use:     "serve",
		Short:   "Starts optimus service",
		Example: "optimus serve",
		Annotations: map[string]string{
			"group:other": "dev",
		},
		RunE: func(c *cli.Command, args []string) error {
			return server.Initialize(l, conf)
		},
	}
	return c
}
