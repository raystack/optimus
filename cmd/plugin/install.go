package plugin

import (
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/plugin"
)

type installCommand struct {
	logger         log.Logger
	serverConfig   *config.ServerConfig
	configFilePath string `default:"config.yaml"`
}

// NewInstallCommand initializes plugin install command
func NewInstallCommand() *cobra.Command {
	install := &installCommand{}
	cmd := &cobra.Command{
		Use:     "install",
		Short:   "install and extract plugins to a dir",
		Example: "optimus plugin install",
		RunE:    install.RunE,
		PreRunE: install.PreRunE,
	}
	cmd.PersistentFlags().StringVarP(&install.configFilePath, "config", "c", install.configFilePath, "File path for server configuration")
	return cmd
}

func (i *installCommand) PreRunE(_ *cobra.Command, _ []string) error {
	c, err := config.LoadServerConfig(i.configFilePath)
	if err != nil {
		return err
	}
	i.serverConfig = c
	i.logger = logger.NewClientLogger(c.Log)
	return nil
}

func (i *installCommand) RunE(_ *cobra.Command, _ []string) error {
	return plugin.InstallPlugins(i.serverConfig)
}
