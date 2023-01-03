package plugin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

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

func TestPlugins(t *testing.T) {
	t.Run("Plugin", func(t *testing.T) {
		t.Run("IsYamlPlugin", func(t *testing.T) {
			binaryTask := NewMockBinaryPlugin("abc", plugin.TypeTask)
			assert.True(t, binaryTask.IsYamlPlugin())

			yamlTask := NewMockYamlPlugin("abc", plugin.TypeTask)
			assert.True(t, yamlTask.IsYamlPlugin())
		})
		t.Run("GetSurveyMethod", func(t *testing.T) {
			binaryTask := NewMockBinaryPlugin("abc", plugin.TypeTask)
			assert.Equal(t, binaryTask.YamlMod, binaryTask.GetSurveyMod())

			yamlTask := NewMockYamlPlugin("abc", plugin.TypeTask)
			assert.Equal(t, yamlTask.YamlMod, yamlTask.GetSurveyMod())
		})
		t.Run("PluginInfoResponse", func(t *testing.T) {
			binaryPlugin := NewMockBinaryPlugin("abc", plugin.TypeTask)
			assert.Equal(t, "abc", binaryPlugin.Info().Name)

			yamlPlugin := NewMockYamlPlugin("abcd", plugin.TypeTask)
			assert.Equal(t, "abcd", yamlPlugin.Info().Name)
		})
	})

	t.Run("ValidatorFactory", func(t *testing.T) {
		validator := plugin.ValidatorFactory.NewFromRegex(`^[a-z0-9_\-]+$`, "invalid string format")
		assert.Error(t, validator(23)) // input should be only string
		assert.Nil(t, validator("abcd"))
	})
	t.Run("PluginsQuestions", func(t *testing.T) {
		testQuest := plugin.Question{
			Name:            "PROJECT",
			Regexp:          `^[a-z0-9_\-]+$`,
			ValidationError: "invalid name",
			MinLength:       3,
			MaxLength:       5,
		}
		assert.Error(t, testQuest.IsValid("ab"))     // error minlength
		assert.Error(t, testQuest.IsValid("abcdef")) // error maxlength
		assert.Error(t, testQuest.IsValid("ABCD"))   // error regexp
		assert.Nil(t, testQuest.IsValid("abc"))      // no error

		testQuest = plugin.Question{
			Name:     "PROJECT",
			Required: true,
		}
		assert.Error(t, testQuest.IsValid("")) // error required
	})
}
