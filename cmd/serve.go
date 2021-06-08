package cmd

import (
	"github.com/odpf/optimus/cmd/server"
	"github.com/odpf/optimus/config"

	cli "github.com/spf13/cobra"
)

func optimusServeCommand(l logger, conf *config.Optimus) *cli.Command {
	c := &cli.Command{
		Use:   "serve",
		Short: "Starts optimus service",
		RunE: func(c *cli.Command, args []string) error {
			return server.Initialize(conf)
		},
	}
	return c
}
