package yaml

import (
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/models"
)

const (
	Prefix = "optimus-plugin-"
	Suffix = ".yaml"
)

func NewPlugin(pluginPath string) (models.CommandLineMod, error) {
	fs := afero.NewOsFs()
	fd, err := fs.Open(pluginPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = models.ErrNoSuchSpec
		}
		return nil, err
	}
	defer fd.Close()

	pluginBytes, err := io.ReadAll(fd)

	if err != nil {
		return nil, err
	}

	plugin := models.PluginSpec{}
	if err := yaml.UnmarshalStrict(pluginBytes, &plugin); err != nil {
		return &plugin, err
	}
	return &plugin, nil
}

// if error in loading, initializing or adding to pluginsrepo , skipping that particular plugin
// NOTE: binary plugins are loaded prior to yaml plugins
func Init(pluginsRepo models.PluginRepository, discoveredYamlPlugins []string, pluginLogger hclog.Logger) {
	for _, yamlPluginPath := range discoveredYamlPlugins {
		yamlPlugin, err := NewPlugin(yamlPluginPath)
		if err != nil {
			pluginLogger.Error(fmt.Sprintf("plugin Init: %s", yamlPluginPath), err)
			continue
		}
		pluginInfo, _ := yamlPlugin.PluginInfo()
		if binPlugin, _ := pluginsRepo.GetByName(pluginInfo.Name); binPlugin != nil {
			// if bin plugin exists skip loading yaml plugin
			continue
		}
		if err := pluginsRepo.Add(nil, nil, nil, yamlPlugin); err != nil {
			pluginLogger.Error(fmt.Sprintf("PluginRegistry.Add: %s", yamlPluginPath), err)
			continue
		}
		pluginLogger.Debug("plugin ready: ", pluginInfo.Name)
	}
}
