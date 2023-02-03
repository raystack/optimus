package yaml

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/sdk/plugin"
)

const (
	Prefix = "optimus-plugin-"
	Suffix = ".yaml"
)

type PluginSpec struct {
	plugin.Info                  `yaml:",inline,omitempty"`
	plugin.GetQuestionsResponse  `yaml:",inline,omitempty"`
	plugin.DefaultAssetsResponse `yaml:",inline,omitempty"`
	plugin.DefaultConfigResponse `yaml:",inline,omitempty"`
}

func (p *PluginSpec) PluginInfo() *plugin.Info {
	return &plugin.Info{
		Name:          p.Name,
		Description:   p.Description,
		Image:         p.Image,
		PluginType:    p.PluginType,
		PluginMods:    []plugin.Mod{plugin.ModTypeCLI},
		PluginVersion: p.PluginVersion,
		HookType:      p.HookType,
		DependsOn:     p.DependsOn,
		APIVersion:    p.APIVersion,
	}
}

func (p *PluginSpec) GetQuestions(context.Context, plugin.GetQuestionsRequest) (*plugin.GetQuestionsResponse, error) {
	return &plugin.GetQuestionsResponse{
		Questions: p.Questions,
	}, nil
}

func (*PluginSpec) ValidateQuestion(_ context.Context, req plugin.ValidateQuestionRequest) (*plugin.ValidateQuestionResponse, error) {
	question := req.Answer.Question
	value := req.Answer.Value
	if err := question.IsValid(value); err != nil {
		return &plugin.ValidateQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &plugin.ValidateQuestionResponse{
		Success: true,
	}, nil
}

func (p *PluginSpec) DefaultConfig(_ context.Context, req plugin.DefaultConfigRequest) (*plugin.DefaultConfigResponse, error) {
	var conf []plugin.Config

	// config from survey answers
	for _, ans := range req.Answers {
		conf = append(conf, plugin.Config{
			Name:  ans.Question.Name,
			Value: ans.Value,
		})
	}

	// adding defaultconfig (static, macros & referential config) from yaml
	conf = append(conf, p.Config...)

	return &plugin.DefaultConfigResponse{
		Config: conf,
	}, nil
}

func (p *PluginSpec) DefaultAssets(context.Context, plugin.DefaultAssetsRequest) (*plugin.DefaultAssetsResponse, error) {
	return &plugin.DefaultAssetsResponse{
		Assets: p.Assets,
	}, nil
}

func NewPluginSpec(pluginPath string) (*PluginSpec, error) {
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
	var plugin PluginSpec
	if err := yaml.Unmarshal(pluginBytes, &plugin); err != nil { // TODO: check if strict marshal is required
		return &plugin, err
	}
	return &plugin, nil
}

// if error in loading, initializing or adding to pluginsrepo , skipping that particular plugin
// NOTE: binary plugins are loaded after yaml plugins loaded
func Init(pluginsRepo *models.PluginRepository, discoveredYamlPlugins []string, pluginLogger hclog.Logger) error {
	for _, yamlPluginPath := range discoveredYamlPlugins {
		yamlPluginSpec, err := NewPluginSpec(yamlPluginPath)
		if err != nil {
			pluginLogger.Error(fmt.Sprintf("plugin Init: %s", yamlPluginPath), err)
			return err
		}
		pluginInfo := yamlPluginSpec.PluginInfo()
		if err := pluginsRepo.AddYaml(yamlPluginSpec); err != nil {
			pluginLogger.Error(fmt.Sprintf("PluginRegistry.Add: %s", yamlPluginPath), err)
			return err
		}
		pluginLogger.Debug("plugin ready: ", pluginInfo.Name)
	}

	// Generic plugin, only server side
	return pluginsRepo.AddYaml(GenericPlugin())
}
