package mock

import (
	"context"

	"github.com/odpf/optimus/sdk/plugin"
)

func NewMockBinaryPlugin(name, pluginType string) *plugin.Plugin {
	return &plugin.Plugin{
		YamlMod:       &MockYamlMod{Name: name, Type: pluginType},
		DependencyMod: &MockDependencyMod{Name: name, Type: pluginType},
	}
}

func NewMockYamlPlugin(name, pluginType string) *plugin.Plugin {
	return &plugin.Plugin{
		YamlMod: &MockYamlMod{Name: name, Type: pluginType},
	}
}

type MockYamlMod struct {
	Name string
	Type string
}

func (p *MockYamlMod) PluginInfo() *plugin.Info {
	return &plugin.Info{
		Name:          p.Name,
		Description:   "Yaml Test Desc",
		PluginType:    plugin.Type(p.Type),
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		PluginMods:    []plugin.Mod{plugin.ModTypeCLI},
	}
}

func (*MockYamlMod) GetQuestions(context.Context, plugin.GetQuestionsRequest) (*plugin.GetQuestionsResponse, error) {
	return &plugin.GetQuestionsResponse{Questions: plugin.Questions{}}, nil
}

func (*MockYamlMod) ValidateQuestion(context.Context, plugin.ValidateQuestionRequest) (*plugin.ValidateQuestionResponse, error) {
	return &plugin.ValidateQuestionResponse{Success: true}, nil
}

func (*MockYamlMod) DefaultConfig(context.Context, plugin.DefaultConfigRequest) (*plugin.DefaultConfigResponse, error) {
	return &plugin.DefaultConfigResponse{Config: plugin.Configs{}}, nil
}

func (*MockYamlMod) DefaultAssets(context.Context, plugin.DefaultAssetsRequest) (*plugin.DefaultAssetsResponse, error) {
	return &plugin.DefaultAssetsResponse{Assets: plugin.Assets{}}, nil
}

type MockDependencyMod struct {
	Name string
	Type string
}

func (*MockDependencyMod) GetName(context.Context) (string, error) {
	return "", nil
}

func (*MockDependencyMod) GenerateDestination(context.Context, plugin.GenerateDestinationRequest) (*plugin.GenerateDestinationResponse, error) {
	return &plugin.GenerateDestinationResponse{}, nil
}

func (*MockDependencyMod) GenerateDependencies(context.Context, plugin.GenerateDependenciesRequest) (*plugin.GenerateDependenciesResponse, error) {
	return &plugin.GenerateDependenciesResponse{}, nil
}

func (*MockDependencyMod) CompileAssets(context.Context, plugin.CompileAssetsRequest) (*plugin.CompileAssetsResponse, error) {
	return &plugin.CompileAssetsResponse{}, nil
}
