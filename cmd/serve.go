package cmd

import (
	"github.com/odpf/optimus/cmd/server"
	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func serveCommand(l log.Logger) *cli.Command {
	c := &cli.Command{
		Use:     "serve",
		Short:   "Starts optimus service",
		Example: "optimus serve",
		Annotations: map[string]string{
			"group:other": "dev",
		},
	}

	// TODO: find a way to load the config in one place
	conf, err := config.LoadServerConfig()
	if err != nil {
		l.Error(err.Error())
		return nil
	}

	c.RunE = func(c *cli.Command, args []string) error {
		return server.Initialize(l, *conf)
	}

	return c
}
