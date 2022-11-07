package plugin

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/base"
	"github.com/odpf/optimus/plugin/v1beta1/dependencyresolver"
	"github.com/odpf/optimus/plugin/yaml"
)

func Initialize(pluginLogger hclog.Logger, arg ...string) error {
	discoveredPlugins := DiscoverPlugins(pluginLogger)
	pluginLogger.Debug(fmt.Sprintf("discovering binary plugins(%d)...", len(discoveredPlugins)))

	// pluginMap is the map of plugins we can dispense.
	pluginMap := map[string]plugin.Plugin{
		models.PluginTypeBase:                     base.NewPluginClient(pluginLogger),
		models.ModTypeDependencyResolver.String(): dependencyresolver.NewPluginClient(pluginLogger),
	}

	for _, pluginPath := range discoveredPlugins {
		// we are core, start by launching the plugin processes
		pluginClient := plugin.NewClient(&plugin.ClientConfig{
			HandshakeConfig:  base.Handshake,
			Plugins:          pluginMap,
			Cmd:              exec.Command(pluginPath, arg...),
			Managed:          true,
			AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
			Logger:           pluginLogger,
		})

		// connect via GRPC
		rpcClient, err := pluginClient.Client()
		if err != nil {
			return fmt.Errorf("client.Client(): %s: %w", pluginPath, err)
		}

		var baseClient models.BasePlugin
		var drClient models.DependencyResolverMod

		// request plugin as base
		raw, err := rpcClient.Dispense(models.PluginTypeBase)
		if err != nil {
			pluginClient.Kill()
			return fmt.Errorf("rpcClient.Dispense: %s: %w", pluginPath, err)
		}
		baseClient = raw.(models.BasePlugin)
		baseInfo, err := baseClient.PluginInfo()
		if err != nil {
			return fmt.Errorf("failed to read plugin info: %s: %w", pluginPath, err)
		}
		pluginLogger.Debug("plugin connection established: ", baseInfo.Name)

		if modSupported(baseInfo.PluginMods, models.ModTypeDependencyResolver) {
			// create a client with dependency resolver mod
			if rawMod, err := rpcClient.Dispense(models.ModTypeDependencyResolver.String()); err == nil {
				drClient = rawMod.(models.DependencyResolverMod)
				pluginLogger.Debug(fmt.Sprintf("%s mod found for: %s", models.ModTypeDependencyResolver, baseInfo.Name))

				// cache name
				drGRPCClient := rawMod.(*dependencyresolver.GRPCClient)
				drGRPCClient.SetName(baseInfo.Name)
			}
		}

		if err := models.PluginRegistry.Add(baseClient, drClient); err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", pluginPath, err)
		}
		pluginLogger.Debug("plugin ready: ", baseInfo.Name)
	}

	// first fetch binary plugins, then add yaml plugin for which binary is not added earlier
	discoveredYamlPlugins := DiscoverPluginsGivenFilePattern(pluginLogger, yaml.Prefix, yaml.Suffix)
	pluginLogger.Debug(fmt.Sprintf("discovering yaml   plugins(%d)...", len(discoveredYamlPlugins)))
	yaml.Init(models.PluginRegistry, discoveredYamlPlugins, pluginLogger)
	return nil
}

func modSupported(mods []models.PluginMod, mod models.PluginMod) bool {
	for _, m := range mods {
		if m == mod {
			return true
		}
	}
	return false
}

func DiscoverPluginsGivenFilePattern(pluginLogger hclog.Logger, prefix, suffix string) []string {
	var discoveredPlugins, dirs []string

	if p, err := os.Getwd(); err == nil {
		dirs = append(dirs, path.Join(p, PluginsDir))
		dirs = append(dirs, p)
	} else {
		pluginLogger.Debug(fmt.Sprintf("Error discovering working dir: %s", err))
	}

	// look in the same directory as the executable
	if exePath, err := os.Executable(); err != nil {
		pluginLogger.Debug(fmt.Sprintf("Error discovering exe directory: %s", err))
	} else {
		dirs = append(dirs, filepath.Dir(exePath))
	}

	// add user home directory
	if currentHomeDir, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(currentHomeDir, ".optimus", "plugins"))
	}
	dirs = append(dirs, []string{"/usr/bin", "/usr/local/bin"}...)

	for _, dirPath := range dirs {
		fileInfos, err := os.ReadDir(dirPath)
		if err != nil {
			continue
		}

		for _, item := range fileInfos {
			fullName := item.Name()

			if !strings.HasPrefix(fullName, prefix) {
				continue
			}
			if !strings.HasSuffix(fullName, suffix) {
				continue
			}

			absPath, err := filepath.Abs(filepath.Join(dirPath, fullName))
			if err != nil {
				continue
			}

			info, err := os.Stat(absPath)
			if err != nil {
				continue
			}
			if info.IsDir() {
				continue
			}

			if len(strings.Split(fullName, "-")) < 2 { //nolint: gomnd
				continue
			}

			// get plugin name
			pluginName := strings.Split(fullName, "_")[0]
			absPath = filepath.Clean(absPath)

			// check for duplicate binaries, could be different versions
			// if we have already discovered one, ignore rest
			isAlreadyFound := false
			for _, storedName := range discoveredPlugins {
				if strings.Contains(storedName, pluginName) {
					isAlreadyFound = true
				}
			}

			if !isAlreadyFound {
				discoveredPlugins = append(discoveredPlugins, absPath)
			}
		}
	}
	return discoveredPlugins
}

// DiscoverPlugins look for plugin binaries in following folders
// order to search is top to down
// ./
// <exec>/
// <exec>/.optimus/plugins
// $HOME/.optimus/plugins
// /usr/bin
// /usr/local/bin
//
// for duplicate binaries(even with different versions for now), only the first found will be used
// sample plugin name: optimus-myplugin_linux_amd64
func DiscoverPlugins(pluginLogger hclog.Logger) []string {
	var (
		prefix = "optimus-"
		suffix = fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH)
	)
	return DiscoverPluginsGivenFilePattern(pluginLogger, prefix, suffix)
}

// Factory returns a new plugin instance
type Factory func(log hclog.Logger) interface{}

// Serve is used to serve a new Nomad plugin
func Serve(f Factory) {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Trace,
		JSONFormat: true,
	})
	servePlugin(f(logger), logger)
}

func servePlugin(optimusPlugin interface{}, logger hclog.Logger) {
	switch p := optimusPlugin.(type) {
	case models.DependencyResolverMod:
		dependencyresolver.Serve(p, logger)
	case models.BasePlugin:
		base.Serve(p, logger)
	default:
		logger.Error("Unsupported plugin type interface")
	}
}
