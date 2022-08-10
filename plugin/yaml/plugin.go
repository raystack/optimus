package yaml

import (
	"context"
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

type PluginSpec struct {
	models.PluginInfoResponse    `yaml:",inline"`
	models.GetQuestionsResponse  `yaml:",inline"` // PluginQuestion has extra attrs related to validation
	models.DefaultAssetsResponse `yaml:",inline"`
	models.DefaultConfigResponse `yaml:",inline"`
}

func (p *PluginSpec) PluginInfo() (*models.PluginInfoResponse, error) { // nolint
	return &models.PluginInfoResponse{
		Name:          p.Name,
		Description:   p.Description,
		Image:         p.Image,
		SecretPath:    p.SecretPath,
		PluginType:    p.PluginType,
		PluginMods:    p.PluginMods,
		PluginVersion: p.PluginVersion,
		HookType:      p.HookType,
		DependsOn:     p.DependsOn,
		APIVersion:    p.APIVersion,
	}, nil
}

func (p *PluginSpec) GetQuestions(context.Context, models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) { //nolint
	return &models.GetQuestionsResponse{
		Questions: p.Questions,
	}, nil
}

func (p *PluginSpec) ValidateQuestion(_ context.Context, req models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) { //nolint
	question := req.Answer.Question
	value := req.Answer.Value
	if err := question.IsValid(value); err != nil {
		return &models.ValidateQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &models.ValidateQuestionResponse{
		Success: true,
	}, nil
}

func (p *PluginSpec) DefaultConfig(_ context.Context, req models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) { //nolint
	conf := []models.PluginConfig{}

	// config from survey answers
	for _, ans := range req.Answers {
		conf = append(conf, models.PluginConfig{
			Name:  ans.Question.Name,
			Value: ans.Value,
		})
	}

	// adding defaultconfig (static, macros & referential config) from yaml
	conf = append(conf, p.Config...)

	return &models.DefaultConfigResponse{
		Config: conf,
	}, nil
}

func (p *PluginSpec) DefaultAssets(context.Context, models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) { //nolint
	return &models.DefaultAssetsResponse{
		Assets: p.Assets,
	}, nil
}

func (PluginSpec) CompileAssets(_ context.Context, req models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) { //nolint
	return &models.CompileAssetsResponse{
		Assets: req.Assets,
	}, nil
}

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
	plugin := PluginSpec{}
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
		if plugin, _ := pluginsRepo.GetByName(pluginInfo.Name); plugin != nil && !plugin.IsYamlPlugin() {
			pluginLogger.Debug("skipping yaml plugin (as binary already added): ", pluginInfo.Name)
			continue
		}
		if err := pluginsRepo.Add(nil, nil, nil, yamlPlugin); err != nil {
			pluginLogger.Error(fmt.Sprintf("PluginRegistry.Add: %s", yamlPluginPath), err)
			continue
		}
		pluginLogger.Debug("plugin ready: ", pluginInfo.Name)
	}
}
