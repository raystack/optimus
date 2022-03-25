package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/odpf/optimus/cmd/server"
	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func serveCommand(l log.Logger, conf config.Optimus) *cli.Command {
	c := &cli.Command{
		Use:     "serve",
		Short:   "Starts optimus service",
		Example: "optimus serve",
		Annotations: map[string]string{
			"group:other": "dev",
		},
		RunE: func(c *cli.Command, args []string) error {
			l.Info(coloredSuccess("Starting Optimus"), "version", config.Version)
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
