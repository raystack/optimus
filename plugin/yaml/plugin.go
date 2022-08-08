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

	// need to use existing models to support CLIMOD Interface
	pluginInfo := models.PluginInfoResponse{}
	pluginQuestions := models.GetQuestionsResponse{}
	pluginYamlQuestions := models.YamlQuestions{}
	pluginDefaultAssets := models.DefaultAssetsResponse{}
	pluginDefaultConfig := models.DefaultConfigResponse{}

	if err := yaml.Unmarshal(pluginBytes, &pluginInfo); err != nil {
		return &plugin, err
	}
	plugin.Info = pluginInfo

	if err := yaml.Unmarshal(pluginBytes, &pluginQuestions); err != nil {
		return &plugin, err
	}
	plugin.Questions = pluginQuestions

	if err := yaml.Unmarshal(pluginBytes, &pluginYamlQuestions); err != nil {
		return &plugin, err
	}
	plugin.YamlQuestions = pluginYamlQuestions

	if err := yaml.Unmarshal(pluginBytes, &pluginDefaultAssets); err != nil {
		return &plugin, err
	}
	plugin.Assets = pluginDefaultAssets

	if err := yaml.Unmarshal(pluginBytes, &pluginDefaultConfig); err != nil {
		return &plugin, err
	}
	plugin.Config = pluginDefaultConfig

	plugin.YamlQuestions.ConstructIndex()

	if errs := validator.Validate(plugin); errs != nil {
		// values not valid, deal with errors here
		return &plugin, fmt.Errorf(fmt.Sprintf("Error at plugin %s", pluginPath), errs)
	}
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
