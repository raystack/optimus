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

func NewYamlPlugin(pluginPath string) (*models.YamlPlugin, error) {
	plugin := models.YamlPlugin{}

	fs := afero.NewOsFs()
	fd, err := fs.Open(pluginPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = models.ErrNoSuchSpec
		}
		return &plugin, err
	}
	defer fd.Close()

	pluginBytes, err := io.ReadAll(fd)

	if err != nil {
		return &plugin, err
	}

	pluginInfo := models.PluginInfoResponse{}
	pluginQuestions := models.GetQuestionsResponse{}
	pluginYamlQuestions := models.YamlQuestions{}
	pluginDefaultAssets := models.YamlAsset{}

	if err := yaml.Unmarshal(pluginBytes, &pluginInfo); err != nil {
		return &plugin, err
	}
	plugin.Info = &pluginInfo
	if err := yaml.Unmarshal(pluginBytes, &pluginQuestions); err != nil {
		return &plugin, err
	}
	plugin.PluginQuestions = &pluginQuestions
	if err := yaml.Unmarshal(pluginBytes, &pluginYamlQuestions); err != nil {
		return &plugin, err
	}
	plugin.YamlQuestions = &pluginYamlQuestions
	if err := yaml.Unmarshal(pluginBytes, &pluginDefaultAssets); err != nil {
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

func Init(pluginsRepo models.PluginRepository, discoveredYamlPlugins []string, pluginLogger hclog.Logger) error {
	for _, yamlPluginPath := range discoveredYamlPlugins {
		yamlPlugin, err := NewYamlPlugin(yamlPluginPath)
		if err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", yamlPluginPath, err)
		}

		if binPlugin, _ := pluginsRepo.GetByName(yamlPlugin.Info.Name); binPlugin != nil {
			// if bin plugin exists skip loading yaml plugin
			continue
		}
		if err := pluginsRepo.Add(nil, nil, nil, yamlPlugin); err != nil {
			return fmt.Errorf("PluginRegistry.Add: %s: %w", yamlPluginPath, err)
		}

		pluginLogger.Debug("plugin ready: ", yamlPlugin.Info.Name)
	}
	return nil
}
