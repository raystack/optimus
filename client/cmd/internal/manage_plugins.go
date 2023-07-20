package internal

import (
	"os"

	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"

	"github.com/raystack/optimus/config"
	"github.com/raystack/optimus/internal/models"
	oPlugin "github.com/raystack/optimus/plugin"
)

// InitPlugins triggers initialization of all available plugins
func InitPlugins(logLevel config.LogLevel) (*models.PluginRepository, error) {
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
