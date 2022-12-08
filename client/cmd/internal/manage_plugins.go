package internal

import (
	"os"

	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	oPlugin "github.com/odpf/optimus/plugin"
)

// InitPlugins triggers initialization of all available plugins
func InitPlugins(logLevel config.LogLevel) (*models.RegisteredPlugins, error) {
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
	pluginRepo, err := oPlugin.Initialize(pluginLogger)
	return pluginRepo, err
}

func CleanupPlugins() {
	hPlugin.CleanupClients()
}
