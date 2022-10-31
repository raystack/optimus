package models_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
)

func NewMockBinaryPlugin(name, pluginType string) *models.Plugin {
	return &models.Plugin{
		Base:          &MockBasePlugin{Name: name, Type: pluginType},
		DependencyMod: &MockDependencyMod{Name: name, Type: pluginType},
	}
}

func NewMockYamlPlugin(name, pluginType string) *models.Plugin {
	return &models.Plugin{
		YamlMod: &MockYamlMod{Name: name, Type: pluginType},
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
	}, nil
}

type MockYamlMod struct {
	Name string
	Type string
}

func (p *MockYamlMod) PluginInfo() (*models.PluginInfoResponse, error) {
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
		PluginMods:    []models.PluginMod{models.ModTypeCLI},
	}, nil
}

func (*MockYamlMod) GetQuestions(context.Context, models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {
	return &models.GetQuestionsResponse{Questions: models.PluginQuestions{}}, nil
}

func (*MockYamlMod) ValidateQuestion(context.Context, models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	return &models.ValidateQuestionResponse{Success: true}, nil
}

func (*MockYamlMod) DefaultConfig(context.Context, models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	return &models.DefaultConfigResponse{Config: models.PluginConfigs{}}, nil
}

func (*MockYamlMod) DefaultAssets(context.Context, models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	return &models.DefaultAssetsResponse{Assets: models.PluginAssets{}}, nil
}

type MockDependencyMod struct {
	Name string
	Type string
}

func (p *MockDependencyMod) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          p.Name,
		Description:   "Binary plugin",
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

func (*MockDependencyMod) GenerateDestination(context.Context, models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	return &models.GenerateDestinationResponse{}, nil
}

func (*MockDependencyMod) GenerateDependencies(context.Context, models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	return &models.GenerateDependenciesResponse{}, nil
}

func (*MockDependencyMod) CompileAssets(context.Context, models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	return &models.CompileAssetsResponse{}, nil
}

func TestPluginModels(t *testing.T) {
	t.Run("Plugin", func(t *testing.T) {
		t.Run("IsYamlPlugin", func(t *testing.T) {
			plugin := NewMockBinaryPlugin("abc", string(models.PluginTypeTask))
			assert.False(t, plugin.IsYamlPlugin())
			plugin = NewMockYamlPlugin("abc", string(models.PluginTypeTask))
			assert.True(t, plugin.IsYamlPlugin())
		})
		t.Run("GetSurveyMethod", func(t *testing.T) {
			plugin := NewMockBinaryPlugin("abc", string(models.PluginTypeTask))
			assert.Equal(t, nil, plugin.GetSurveyMod())
			plugin = NewMockYamlPlugin("abc", string(models.PluginTypeTask))
			assert.Equal(t, plugin.YamlMod, plugin.GetSurveyMod())
		})
		t.Run("PluginInfoResponse", func(t *testing.T) {
			plugin := NewMockBinaryPlugin("abc", string(models.PluginTypeTask))
			yamlPlugin := NewMockYamlPlugin("abcd", string(models.PluginTypeTask))
			assert.Equal(t, "abc", plugin.Info().Name)
			assert.Equal(t, "abcd", yamlPlugin.Info().Name)
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
			NewMockYamlPlugin("c", string(models.PluginTypeTask)),
			NewMockYamlPlugin("b", string(models.PluginTypeTask)),
			NewMockBinaryPlugin("a", string(models.PluginTypeHook)),
			NewMockYamlPlugin("a", string(models.PluginTypeHook)),
			NewMockYamlPlugin("z", string(models.PluginTypeTask)),
		}
		for _, plugin := range plugins {
			if plugin.IsYamlPlugin() {
				repo.AddYaml(plugin.YamlMod)
			} else {
				repo.Add(plugin.Base, plugin.DependencyMod)
			}
		}
		t.Run("should allow both yaml and bin implementations in plugin", func(t *testing.T) {
			plugin, _ := repo.GetByName("a")
			assert.Equal(t, plugin.IsYamlPlugin(), true)
			assert.NotNil(t, plugin.YamlMod)
		})

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
	})
}
