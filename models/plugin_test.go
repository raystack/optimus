package models_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
)

func NewMockPlugin(name, pluginType string) *models.Plugin {
	return &models.Plugin{
		Base:    &MockBasePlugin{Name: name, Type: pluginType},
		CLIMod:  &MockCLIMod{Name: name, Type: pluginType, IsYamlMod: false},
		YamlMod: &MockCLIMod{Name: name, Type: pluginType, IsYamlMod: true},
	}
}

func NewMockYamlPlugin(name, pluginType string) *models.Plugin {
	return &models.Plugin{
		Base:    nil,
		CLIMod:  nil,
		YamlMod: &MockCLIMod{Name: name, Type: pluginType, IsYamlMod: true},
	}
}

type MockBasePlugin struct {
	Name string
	Type string
}

func (p *MockBasePlugin) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          p.Name,
		Description:   "BigQuery to BigQuery transformation task",
		PluginType:    models.PluginType(p.Type),
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		SecretPath:    "/tmp/auth.json",
		PluginMods:    []models.PluginMod{models.ModTypeDependencyResolver},
	}, nil
}

type MockCLIMod struct {
	Name      string
	Type      string
	IsYamlMod bool
}

func (p *MockCLIMod) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          p.Name,
		Description:   "Yaml Test Desc",
		PluginType:    models.PluginType(p.Type),
		PluginVersion: "dev",
		APIVersion:    nil,
		DependsOn:     nil,
		HookType:      "",
		Image:         "gcr.io/bq-plugin:dev",
		SecretPath:    "/tmp/auth.json",
		PluginMods:    []models.PluginMod{models.ModTypeDependencyResolver},
	}, nil
}

func (*MockCLIMod) GetQuestions(context.Context, models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {
	return &models.GetQuestionsResponse{Questions: models.PluginQuestions{}}, nil
}

func (*MockCLIMod) ValidateQuestion(context.Context, models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	return &models.ValidateQuestionResponse{Success: true}, nil
}

func (*MockCLIMod) DefaultConfig(context.Context, models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	return &models.DefaultConfigResponse{Config: models.PluginConfigs{}}, nil
}

func (*MockCLIMod) DefaultAssets(context.Context, models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	return &models.DefaultAssetsResponse{Assets: models.PluginAssets{}}, nil
}

func (MockCLIMod) CompileAssets(_ context.Context, req models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	return &models.CompileAssetsResponse{Assets: req.Assets}, nil
}

func TestPluginModels(t *testing.T) {
	t.Run("Plugin", func(t *testing.T) {
		t.Run("IsYamlPlugin", func(t *testing.T) {
			plugin := NewMockPlugin("abc", string(models.PluginTypeTask))
			assert.Equal(t, plugin.IsYamlPlugin(), false)
			plugin = NewMockYamlPlugin("abc", string(models.PluginTypeTask))
			assert.Equal(t, plugin.IsYamlPlugin(), true)
		})
		t.Run("GetSurveyMethod", func(t *testing.T) {
			plugin := NewMockPlugin("abc", string(models.PluginTypeTask))
			assert.Equal(t, plugin.GetSurveyMod(), plugin.CLIMod)
			plugin = NewMockYamlPlugin("abc", string(models.PluginTypeTask))
			assert.Equal(t, plugin.GetSurveyMod(), plugin.YamlMod)
		})
		t.Run("PluginInfoResponse", func(t *testing.T) {
			plugin := NewMockPlugin("abc", string(models.PluginTypeTask))
			yamlPlugin := NewMockYamlPlugin("abcd", string(models.PluginTypeTask))
			assert.Equal(t, plugin.GetSurveyMod(), plugin.CLIMod)
			assert.Equal(t, yamlPlugin.GetSurveyMod(), yamlPlugin.YamlMod)
		})
	})

	t.Run("ValidatorFactory", func(t *testing.T) {
		validator := models.ValidatorFactory.NewFromRegex(`^[a-z0-9_\-]+$`, "invalid string format")
		assert.Error(t, validator(23)) // input should be only string
		assert.Nil(t, validator("abcd"))
	})
	t.Run("PluginsQuestions", func(t *testing.T) {
		testQuest := models.PluginQuestion{
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

		testQuest = models.PluginQuestion{
			Name:     "PROJECT",
			Required: true,
		}
		assert.Error(t, testQuest.IsValid("")) // error required
	})
	t.Run("PluginRegistry", func(t *testing.T) {
		repo := models.NewPluginRepository()
		plugins := []*models.Plugin{
			NewMockPlugin("c", string(models.PluginTypeTask)),
			NewMockPlugin("b", string(models.PluginTypeTask)),
			NewMockPlugin("a", string(models.PluginTypeHook)),
			NewMockYamlPlugin("z", string(models.PluginTypeTask)),
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
