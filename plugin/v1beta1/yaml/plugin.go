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
	pluginDefaultAssets := models.YamlAsset{}

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
	if err := yaml.Unmarshal(plugin_bytes, &pluginDefaultAssets); err != nil {
		return &plugin, err
	}
	plugin.PluginAssets = &pluginDefaultAssets

	plugin.YamlQuestions.ConstructIndex()
	return &plugin, nil
}

// func printYaml(s interface{}) {
// 	yamlData, err := yaml.Marshal(&s)
// 	if err != nil {
// 		fmt.Printf("Error while Marshaling. %v", err)
// 	}
// 	fmt.Println(" --- YAML ---")
// 	fmt.Println(string(yamlData))
// }

func Init(plugins_repo models.PluginRepository, discoveredYamlPlugins []string, pluginLogger hclog.Logger) error {
	for _, yamlPluginPath := range discoveredYamlPlugins {
		yamlPlugin, err := FromYaml(yamlPluginPath)
		if err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", yamlPluginPath, err)
		}

		if binPlugin, _ := plugins_repo.GetByName(yamlPlugin.Info.Name); binPlugin != nil {
			// if bin plugin exists
			continue
		}
		if err := plugins_repo.Add(nil, nil, nil, yamlPlugin); err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", yamlPluginPath, err)
		}

		// if err := models.YamlPluginRegistry.Add(yamlPlugin); err != nil {
		// 	return fmt.Errorf("PluginRegistry.Add: %s: %w", yamlPluginPath, err)
		// }
		pluginLogger.Debug("plugin ready: ", yamlPlugin.Info.Name)
	}
	return nil
}
