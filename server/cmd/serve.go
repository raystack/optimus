package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/raystack/optimus/config"
	oplugin "github.com/raystack/optimus/plugin"
	"github.com/raystack/optimus/server"
)

type serveCommand struct {
	configFilePath string
	installPlugins bool
}

// NewServeCommand initializes command to start server
func NewServeCommand() *cobra.Command {
	serve := &serveCommand{}

	cmd := &cobra.Command{
		Use:     "serve",
		Short:   "Starts optimus service",
		Example: "optimus serve",
		Annotations: map[string]string{
			"group:other": "dev",
		},
		RunE: serve.RunE,
	}
	cmd.Flags().BoolVar(&serve.installPlugins, "install-plugins", serve.installPlugins, "install plugins")
	cmd.Flags().StringVarP(&serve.configFilePath, "config", "c", serve.configFilePath, "File path for server configuration")
	return cmd
}

func (s *serveCommand) RunE(_ *cobra.Command, _ []string) error {
	// TODO: find a way to load the config in one place
	conf, err := config.LoadServerConfig(s.configFilePath)
	if err != nil {
		return err
	}

	if s.installPlugins {
		if err := oplugin.InstallPlugins(conf); err != nil {
			return fmt.Errorf("unable to install plugins at server: %w", err)
		}
	}

	optimusServer, err := server.New(conf)
	defer optimusServer.Shutdown()
	if err != nil {
		return fmt.Errorf("unable to create server: %w", err)
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc
	return nil
}
