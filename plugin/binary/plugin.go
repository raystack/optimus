package binary

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/dependencyresolver"
)

var (
	Prefix = "optimus-"
	Suffix = fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH)
)

func Init(pluginsRepo models.PluginRepository, discoveredBinaryPlugins []string, pluginLogger hclog.Logger, args ...string) error {
	// pluginMap is the map of plugins we can dispense.
	pluginMap := map[string]plugin.Plugin{
		models.ModTypeDependencyResolver.String(): dependencyresolver.NewPluginClient(pluginLogger),
	}

	for _, pluginPath := range discoveredBinaryPlugins {
		// we are core, start by launching the plugin processes
		pluginClient := plugin.NewClient(&plugin.ClientConfig{
			HandshakeConfig:  dependencyresolver.Handshake,
			Plugins:          pluginMap,
			Cmd:              exec.Command(pluginPath, args...),
			Managed:          true,
			AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
			Logger:           pluginLogger,
		})

		pluginName := getNameFromPluginPath(pluginPath)

		// connect via GRPC
		rpcClient, err := pluginClient.Client()
		if err != nil {
			return fmt.Errorf("client.Client(): %s: %w", pluginPath, err)
		}
		pluginLogger.Debug("plugin connection established: ", pluginName)

		var drClient models.DependencyResolverMod
		// create a client with dependency resolver mod
		if rawMod, err := rpcClient.Dispense(models.ModTypeDependencyResolver.String()); err != nil {
			return fmt.Errorf("rpcClient.Dispense(): %s: %w", pluginName, err)
		} else {
			drClient = rawMod.(models.DependencyResolverMod)
			pluginLogger.Debug(fmt.Sprintf("%s mod found for: %s", models.ModTypeDependencyResolver, pluginName))

			drGRPCClient := rawMod.(*dependencyresolver.GRPCClient)
			drGRPCClient.SetName(pluginName)

			if err := models.PluginRegistry.AddBinary(pluginName, drClient); err != nil {
				return fmt.Errorf("PluginRegistry.Add: %s: %w", pluginName, err)
			}
			pluginLogger.Debug("plugin ready: ", pluginPath)
		}
	}

	return nil
}

func getNameFromPluginPath(pluginPath string) string {
	fileName := filepath.Base(pluginPath)
	return strings.TrimRight(strings.TrimLeft(fileName, Prefix), Suffix)
}
