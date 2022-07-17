package plugin

import (
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
)

type installCommand struct {
	logger       log.Logger
	serverConfig *config.ServerConfig
}

// NewInstallCommand initializes plugin install command
func NewInstallCommand(serverConfig *config.ServerConfig) *cobra.Command {
	install := &installCommand{
		serverConfig: serverConfig,
	}
	cmd := &cobra.Command{
		Use:     "install",
		Short:   "install and extract plugins to a dir",
		Example: "optimus plugin install",
		RunE:    install.RunE,
		PreRunE: install.PreRunE,
	}
	return cmd
}

func (i *installCommand) PreRunE(_ *cobra.Command, _ []string) error {
	i.logger = logger.NewClientLogger(i.serverConfig.Log)
	return nil
}

// also used during server start
func InstallPlugins(conf *config.ServerConfig, logger log.Logger) error {
	dst := conf.Plugin.Dir
	sources := conf.Plugin.Artifacts
	pluginManger := NewPluginManager(logger)
	return pluginManger.Install(dst, sources...)
}

func (i *installCommand) RunE(_ *cobra.Command, _ []string) error {
	return InstallPlugins(i.serverConfig, i.logger)
}
