package yaml

import (
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/spf13/afero"
	"gopkg.in/validator.v2"
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
	pluginDefaultAssets := models.DefaultAssetsResponse{}
	// pluginConfigs := models.YamlConfig{}

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

// if error in loading, initializing or adding to pluginsrepo , skipping that particular plugin
func Init(pluginsRepo models.PluginRepository, discoveredYamlPlugins []string, pluginLogger hclog.Logger) {
	for _, yamlPluginPath := range discoveredYamlPlugins {
		yamlPlugin, err := NewYamlPlugin(yamlPluginPath)
		if err != nil {
			pluginLogger.Error(fmt.Sprintf("plugin Init: %s", yamlPluginPath), err)
			continue
		}
		if errs := validator.Validate(yamlPlugin); errs != nil {
			// values not valid, deal with errors here
			pluginLogger.Error(fmt.Sprintf("Error at plugin : %s", yamlPluginPath), errs)
			continue

		}

		if binPlugin, _ := pluginsRepo.GetByName(yamlPlugin.Info.Name); binPlugin != nil {
			// if bin plugin exists skip loading yaml plugin
			continue
		}
		if err := pluginsRepo.Add(nil, nil, nil, yamlPlugin); err != nil {
			pluginLogger.Error(fmt.Sprintf("PluginRegistry.Add: %s", yamlPluginPath), err)
			continue
		}
		pluginLogger.Debug("plugin ready: ", yamlPlugin.Info.Name)
	}
}
