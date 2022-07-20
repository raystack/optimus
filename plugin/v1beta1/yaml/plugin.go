package yaml

import (
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/odpf/optimus/models"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

const (
	Prefix = "optimus-plugin-"
	Suffix = ".yaml"
)

func FromYaml(plugin_path string) (*models.YamlPlugin, error) {
	plugin := models.YamlPlugin{}

	fs := afero.NewOsFs()
	fd, err := fs.Open(plugin_path)
	if err != nil {
		if os.IsNotExist(err) {
			err = models.ErrNoSuchSpec
		}
		return &plugin, err
	}
	defer fd.Close()

	plugin_bytes, err := io.ReadAll(fd)

	if err != nil {
		return &plugin, err
	}

	pluginInfo := models.PluginInfoResponse{}
	pluginQuestions := models.GetQuestionsResponse{}
	pluginYamlQuestions := models.YamlQuestions{}

	if err := yaml.Unmarshal(plugin_bytes, &pluginInfo); err != nil {
		return &plugin, err
	}
	plugin.Info = &pluginInfo
	if err := yaml.Unmarshal(plugin_bytes, &pluginQuestions); err != nil {
		return &plugin, err
	}
	plugin.PluginQuestions = &pluginQuestions
	if err := yaml.Unmarshal(plugin_bytes, &pluginYamlQuestions); err != nil {
		return &plugin, err
	}
	plugin.YamlQuestions = &pluginYamlQuestions
	return &plugin, nil
}

func Init(discoveredPlugins []string, plugin_repo *models.IYamlPluginRepository, pluginLogger hclog.Logger) error {
	for _, pluginPath := range discoveredPlugins {
		plugin, _ := FromYaml(pluginPath)
		if err := models.YamlPluginRegistry.Add(plugin); err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", pluginPath, err)
		}
		pluginLogger.Debug("plugin ready: ", plugin.Info.Name)
	}
	return nil
}
