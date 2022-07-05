package plugin

import (
	"os"

	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	oPlugin "github.com/odpf/optimus/plugin"
)

type pluginCommand struct {
	configFilePath string
	serverConfig   *config.ServerConfig
}

// NewPluginCommand initializes command for plugin
func NewPluginCommand() *cobra.Command {
	plugin := pluginCommand{
		serverConfig: &config.ServerConfig{},
	}
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Manage plugins",
		Annotations: map[string]string{
			"group:dev": "true",
		},
		PersistentPreRunE: plugin.PersistentPreRunE,
	}
	cmd.PersistentFlags().StringVarP(&plugin.configFilePath, "config", "c", plugin.configFilePath, "File path for server configuration")
	cmd.AddCommand(NewInstallCommand(plugin.serverConfig))
	return cmd
}

func (p *pluginCommand) PersistentPreRunE(_ *cobra.Command, _ []string) error {
	c, err := config.LoadServerConfig(p.configFilePath)
	if err != nil {
		return err
	}
	*p.serverConfig = *c
	return nil
}

// TriggerClientPluginsInit triggers initialization of all available plugins
func TriggerClientPluginsInit(logLevel config.LogLevel) (cleanFn func(), err error) {
	pluginLogLevel := hclog.Info
	if logLevel == config.LogLevelDebug {
		pluginLogLevel = hclog.Debug
	}

	pluginLoggerOpt := &hclog.LoggerOptions{
		Name:   "optimus",
		Output: os.Stdout,
		Level:  pluginLogLevel,
	}
	pluginLogger := hclog.New(pluginLoggerOpt)

	// discover and load plugins.
	err = oPlugin.Initialize(pluginLogger)
	return hPlugin.CleanupClients, err
}
