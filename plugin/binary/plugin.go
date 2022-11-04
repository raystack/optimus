package binary

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"

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

		// connect via GRPC
		rpcClient, err := pluginClient.Client()
		if err != nil {
			return fmt.Errorf("client.Client(): %s: %w", pluginPath, err)
		}
		pluginLogger.Debug("plugin connection established: ", pluginPath)

		var drClient models.DependencyResolverMod
		// create a client with dependency resolver mod
		rawMod, err := rpcClient.Dispense(models.ModTypeDependencyResolver.String())
		if err != nil {
			return fmt.Errorf("rpcClient.Dispense(): %s: %w", pluginPath, err)
		}
		drClient = rawMod.(models.DependencyResolverMod)
		pluginLogger.Debug(fmt.Sprintf("%s mod found for: %s", models.ModTypeDependencyResolver, pluginPath))

		pluginName, err := drClient.GetName(context.Background())
		if err != nil {
			return fmt.Errorf("drClient.GetName(): %w", err)
		}

		if err := pluginsRepo.AddBinary(drClient); err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", pluginName, err)
		}
		pluginLogger.Debug("plugin ready: ", pluginName)
	}

	return nil
}
