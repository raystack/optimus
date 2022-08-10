package models

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewMockPlugin(name, pluginType string) *Plugin {
	return &Plugin{
		Base:    &MockBasePlugin{Name: name, Type: pluginType},
		CLIMod:  &MockCLIMod{Name: name, Type: pluginType, IsYamlMod: false},
		YamlMod: &MockCLIMod{Name: name, Type: pluginType, IsYamlMod: true},
	}
}

func NewMockYamlPlugin(name, pluginType string) *Plugin {
	return &Plugin{
		Base:    nil,
		CLIMod:  nil,
		YamlMod: &MockCLIMod{Name: name, Type: pluginType, IsYamlMod: true},
	}
}

type MockBasePlugin struct {
	Name string
	Type string
}

func (p *MockBasePlugin) PluginInfo() (*PluginInfoResponse, error) {
	return &PluginInfoResponse{
		Name:          p.Name,
		Description:   "BigQuery to BigQuery transformation task",
		PluginType:    PluginType(p.Type),
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		SecretPath:    "/tmp/auth.json",
		PluginMods:    []PluginMod{ModTypeDependencyResolver},
	}, nil
}

type MockCLIMod struct {
	Name      string
	Type      string
	IsYamlMod bool
}

func (p *MockCLIMod) PluginInfo() (*PluginInfoResponse, error) {
	return &PluginInfoResponse{
		Name:          p.Name,
		Description:   "Yaml Test Desc",
		PluginType:    PluginType(p.Type),
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		SecretPath:    "/tmp/auth.json",
		PluginMods:    []PluginMod{ModTypeDependencyResolver},
	}, nil
}

func (p *MockCLIMod) GetQuestions(context.Context, GetQuestionsRequest) (*GetQuestionsResponse, error) { //nolint
	return &GetQuestionsResponse{Questions: PluginQuestions{}}, nil
}

func (p *MockCLIMod) ValidateQuestion(_ context.Context, req ValidateQuestionRequest) (*ValidateQuestionResponse, error) { //nolint
	return &ValidateQuestionResponse{Success: true}, nil
}

func (p *MockCLIMod) DefaultConfig(_ context.Context, req DefaultConfigRequest) (*DefaultConfigResponse, error) { //nolint
	return &DefaultConfigResponse{Config: PluginConfigs{}}, nil
}

func (p *MockCLIMod) DefaultAssets(context.Context, DefaultAssetsRequest) (*DefaultAssetsResponse, error) { //nolint
	return &DefaultAssetsResponse{Assets: PluginAssets{}}, nil
}

func (MockCLIMod) CompileAssets(_ context.Context, req CompileAssetsRequest) (*CompileAssetsResponse, error) { //nolint
	return &CompileAssetsResponse{Assets: req.Assets}, nil
}

func TestPluginModels(t *testing.T) {
	t.Run("Plugin", func(t *testing.T) {
		t.Run("IsYamlPlugin", func(t *testing.T) {
			plugin := NewMockPlugin("abc", string(PluginTypeTask))
			assert.Equal(t, plugin.IsYamlPlugin(), false)
			plugin = NewMockYamlPlugin("abc", string(PluginTypeTask))
			assert.Equal(t, plugin.IsYamlPlugin(), true)
		})
		t.Run("GetSurveyMethod", func(t *testing.T) {
			plugin := NewMockPlugin("abc", string(PluginTypeTask))
			assert.Equal(t, plugin.GetSurveyMod(), plugin.CLIMod)
			plugin = NewMockYamlPlugin("abc", string(PluginTypeTask))
			assert.Equal(t, plugin.GetSurveyMod(), plugin.YamlMod)
		})
		t.Run("PluginInfoResponse", func(t *testing.T) {
			plugin := NewMockPlugin("abc", string(PluginTypeTask))
			yamlPlugin := NewMockYamlPlugin("abcd", string(PluginTypeTask))
			assert.Equal(t, plugin.GetSurveyMod(), plugin.CLIMod)
			assert.Equal(t, yamlPlugin.GetSurveyMod(), yamlPlugin.YamlMod)
		})
	})

	t.Run("ValidatorFactory", func(t *testing.T) {
		validator := ValidatorFactory.NewFromRegex(`^[a-z0-9_\-]+$`, "invalid string format")
		assert.Error(t, validator(23)) // input should be only string
		assert.Nil(t, validator("abcd"))
	})
	t.Run("PluginsQuestions", func(t *testing.T) {
		testQuest := PluginQuestion{
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

		testQuest = PluginQuestion{
			Name:     "PROJECT",
			Required: true,
		}
		assert.Error(t, testQuest.IsValid("")) // error required
	})
	t.Run("PluginRegistry", func(t *testing.T) {
		repo := NewPluginRepository()
		plugins := []*Plugin{
			NewMockPlugin("c", string(PluginTypeTask)),
			NewMockPlugin("b", string(PluginTypeTask)),
			NewMockPlugin("a", string(PluginTypeHook)),
			NewMockYamlPlugin("z", string(PluginTypeTask)),
		}
		for _, plugin := range plugins {
			repo.Add(plugin.Base, plugin.CLIMod, nil, plugin.YamlMod)
		}

		t.Run("should return sorted plugins", func(t *testing.T) {
			list := repo.GetAll()
			assert.Equal(t, list[0].Info().Name, "a")
			assert.Equal(t, list[1].Info().Name, "b")
			assert.Equal(t, list[2].Info().Name, "c")

			list = repo.GetTasks()
			assert.Equal(t, list[0].Info().Name, "b")

			list = repo.GetHooks()
			assert.Equal(t, list[0].Info().Name, "a")
		})

		t.Run("GetCommandLines", func(t *testing.T) {
			clis := repo.GetCommandLines()
			assert.NotEmpty(t, clis)
			assert.Len(t, clis, len(plugins))
		})
	})
}
