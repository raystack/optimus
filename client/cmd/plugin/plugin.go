package plugin

import (
	"os"

	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	oPlugin "github.com/odpf/optimus/plugin"
)

func NewPluginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Manage plugins",
		Annotations: map[string]string{
			"group:dev": "true",
		},
	}
	cmd.AddCommand(
		NewInstallCommand(),
		NewValidateCommand(),
		NewSyncCommand(),
	)
	return cmd
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
