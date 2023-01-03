package plugin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/hashicorp/go-hclog"

	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/plugin/binary"
	"github.com/odpf/optimus/plugin/v1beta1/dependencyresolver"
	"github.com/odpf/optimus/plugin/yaml"
	"github.com/odpf/optimus/sdk/plugin"
)

func Initialize(pluginLogger hclog.Logger, arg ...string) (*models.PluginRepository, error) {
	pluginRepository := models.NewPluginRepository()
	// fetch yaml plugins first, it holds detailed information about the plugin
	discoveredYamlPlugins := discoverPluginsGivenFilePattern(pluginLogger, yaml.Prefix, yaml.Suffix)
	pluginLogger.Debug(fmt.Sprintf("discovering yaml   plugins(%d)...", len(discoveredYamlPlugins)))
	if err := yaml.Init(pluginRepository, discoveredYamlPlugins, pluginLogger); err != nil {
		return pluginRepository, err
	}

	// fetch binary plugins. Any binary plugin which doesn't have its yaml version will be failed
	discoveredBinaryPlugins := discoverPluginsGivenFilePattern(pluginLogger, binary.Prefix, binary.Suffix)
	pluginLogger.Debug(fmt.Sprintf("discovering binary   plugins(%d)...", len(discoveredBinaryPlugins)))
	err := binary.Init(pluginRepository, discoveredBinaryPlugins, pluginLogger, arg...)

	return pluginRepository, err
}

// discoverPluginsGivenFilePattern look for plugin with the specific pattern in following folders
// order to search is top to down
// ./
// <exec>/
// <exec>/.optimus/plugins
// $HOME/.optimus/plugins
// /usr/bin
// /usr/local/bin
//
// for duplicate plugins(even with different versions for now), only the first found will be used
// sample plugin name:
// - optimus-myplugin_linux_amd64 | with suffix: optimus- and prefix: _linux_amd64
// - optimus-plugin-myplugin.yaml | with suffix: optimus-plugin and prefix: .yaml
func discoverPluginsGivenFilePattern(pluginLogger hclog.Logger, prefix, suffix string) []string {
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
	case plugin.DependencyResolverMod:
		dependencyresolver.Serve(p, logger)
	default:
		logger.Error("Unsupported plugin type interface")
	}
}
