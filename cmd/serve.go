package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/server"
	"github.com/odpf/optimus/config"
)

func serveCommand(l log.Logger) *cli.Command {
	c := &cli.Command{
		Use:     "serve",
		Short:   "Starts optimus service",
		Example: "optimus serve",
		Annotations: map[string]string{
			"group:other": "dev",
		},
		RunE: func(c *cli.Command, args []string) error {
			// TODO: find a way to load the config in one place
			conf, err := config.LoadServerConfig()
			if err != nil {
				l.Error(err.Error())
				return nil
			}

			l.Info(coloredSuccess("Starting Optimus"), "version", config.BuildVersion)
			optimusServer, err := server.New(l, conf)
			defer optimusServer.Shutdown()
			if err != nil {
				return fmt.Errorf("unable to create server: %w", err)
			}

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
			<-sigc
			l.Info(coloredNotice("Shutting down server"))
			return nil
		},
	}

	return c
}
