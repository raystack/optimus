package cmd

import (
	"os"

	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/plugin"
)

// TODO: need to refactor this along side with refactoring the client commands
// Plugin's helper to initialize plugins on client side
func initializeClientPlugins(logLevel config.LogLevel) (cleanFn func(), err error) {
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
	err = plugin.Initialize(pluginLogger)
	return hPlugin.CleanupClients, err
}
