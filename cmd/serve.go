package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/server"
	"github.com/odpf/optimus/config"
)

func serveCommand() *cli.Command {
	var configFilePath string
	cmd := &cli.Command{
		Use:     "serve",
		Short:   "Starts optimus service",
		Example: "optimus serve",
		Annotations: map[string]string{
			"group:other": "dev",
		},
	}

	cmd.Flags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.RunE = func(c *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		conf, err := config.LoadServerConfig(configFilePath)
		if err != nil {
			return err
		}

		optimusServer, err := server.New(*conf)
		defer optimusServer.Shutdown()
		if err != nil {
			return fmt.Errorf("unable to create server: %w", err)
		}

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		<-sigc
		return nil
	}

	return cmd
}
