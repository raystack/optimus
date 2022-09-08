package yaml_test

import (
	"context"
	"os"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/yaml"
)

type mockBasePlugin struct {
	Name          string
	Image         string
	PluginVersion string
	PluginType    string
}

func (p *mockBasePlugin) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          p.Name,
		Image:         p.Image,
		PluginVersion: p.PluginVersion,
		PluginType:    models.PluginType(p.PluginType),
	}, nil
}

func TestYamlPlugin(t *testing.T) {
	testYamlPluginPath := "tests/sample_plugin.yaml" // success
	testYamlPluginName := "bq2bqtest"
	expectedInfo := &models.PluginInfoResponse{
		Name:          "bq2bqtest",
		Description:   "Testing",
		Image:         "docker.io/odpf/optimus-task-bq2bq-executor:latest",
		SecretPath:    "/tmp/auth.json",
		PluginType:    "task",
		PluginMods:    []models.PluginMod{"cli"},
		PluginVersion: "latest",
		HookType:      "",
		DependsOn:     []string(nil),
		APIVersion:    []string(nil),
	}
	testPluginQuestion := models.PluginQuestion{
		Name:      "PROJECT",
		Prompt:    "Project ID",
		Regexp:    `^[a-zA-Z0-9_\-]+$`,
		MinLength: 3,
	}
	expectedQuestions := &models.GetQuestionsResponse{
		Questions: models.PluginQuestions{
			testPluginQuestion,
		},
	}
	expectedAssets := &models.DefaultAssetsResponse{
		Assets: models.PluginAssets{
			models.PluginAsset{
				Name:  "query.sql",
				Value: `Select * from "project.dataset.table";`,
			},
		},
	}

	t.Run("PluginSpec", func(t *testing.T) {
		plugin, _ := yaml.NewPluginSpec(testYamlPluginPath)
		t.Run("PluginInfo", func(t *testing.T) {
			actual, _ := plugin.PluginInfo()
			assert.Equal(t, expectedInfo, actual)
		})
		t.Run("GetQuestions", func(t *testing.T) {
			ctx := context.Background()
			questReq := models.GetQuestionsRequest{JobName: "test"}
			actual, err := plugin.GetQuestions(ctx, questReq)
			assert.Nil(t, err)
			assert.Equal(t, expectedQuestions, actual)
		})

		t.Run("ValidateQuestionSuccess", func(t *testing.T) {
			ctx := context.Background()
			req := models.ValidateQuestionRequest{
				Answer: models.PluginAnswer{
					Question: testPluginQuestion,
					Value:    "test_project",
				},
			}
			actual, err := plugin.ValidateQuestion(ctx, req)
			expected := models.ValidateQuestionResponse{Success: true}
			assert.Nil(t, err)
			assert.Equal(t, expected.Success, actual.Success)
		})
		t.Run("ValidateQuestionFailure", func(t *testing.T) {
			ctx := context.Background()
			req := models.ValidateQuestionRequest{
				Answer: models.PluginAnswer{
					Question: testPluginQuestion,
					Value:    "",
				},
			}
			actual, err := plugin.ValidateQuestion(ctx, req)
			expected := models.ValidateQuestionResponse{Success: false}
			assert.Nil(t, err)
			assert.Equal(t, expected.Success, actual.Success)
		})
		t.Run("DefaultConfig", func(t *testing.T) {
			ctx := context.Background()
			req := models.DefaultConfigRequest{
				Answers: models.PluginAnswers{
					models.PluginAnswer{
						Question: models.PluginQuestion{Name: "PROJECT"},
						Value:    "test_project",
					},
				},
			}
			actual, err := plugin.DefaultConfig(ctx, req)
			expected := &models.DefaultConfigResponse{
				Config: models.PluginConfigs{
					models.PluginConfig{
						Name:  "PROJECT",
						Value: "test_project",
					},
					models.PluginConfig{
						Name:  "TEST",
						Value: "{{.test}}",
					},
				},
			}
			assert.Nil(t, nil, err)
			assert.Equal(t, expected, actual)
		})
		t.Run("DefaultAssets", func(t *testing.T) {
			ctx := context.Background()
			req := models.DefaultAssetsRequest{}
			actual, err := plugin.DefaultAssets(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, expectedAssets, actual)
		})
		t.Run("CompileAssets", func(t *testing.T) {
			ctx := context.Background()
			req := models.CompileAssetsRequest{}
			actual, err := plugin.CompileAssets(ctx, req)
			assert.Nil(t, err)
			assert.Empty(t, actual.Assets)
		})
	})

	t.Run("PluginsInitialization", func(t *testing.T) {
		pluginLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "optimus",
			Output: os.Stdout,
		})
		t.Run("should load plugin for valid paths", func(t *testing.T) {
			repo := models.NewPluginRepository()
			yaml.Init(repo, []string{testYamlPluginPath}, pluginLogger)
			assert.NotEmpty(t, repo.GetAll())
		})
		t.Run("should load yaml even when binary plugin with same name exists", func(t *testing.T) {
			repoWithBinayPlugin := models.NewPluginRepository()
			err := repoWithBinayPlugin.Add(&mockBasePlugin{
				Name:          testYamlPluginName,
				Image:         "sdsd",
				PluginVersion: "asdasd",
				PluginType:    string(models.PluginTypeTask),
			}, nil, nil)
			assert.Nil(t, err)
			assert.Len(t, repoWithBinayPlugin.GetAll(), 1)

			yaml.Init(repoWithBinayPlugin, []string{testYamlPluginPath}, pluginLogger)
			repoPlugins := repoWithBinayPlugin.GetAll()

			assert.Len(t, repoPlugins, 1)
			assert.Equal(t, repoPlugins[0].Info().Name, testYamlPluginName)
			assert.NotNil(t, repoPlugins[0].YamlMod)
		})
		t.Run("should not load duplicate yaml", func(t *testing.T) {
			repoWithBinayPlugin := models.NewPluginRepository()
			yaml.Init(repoWithBinayPlugin, []string{testYamlPluginPath, testYamlPluginPath}, pluginLogger)
			repoPlugins := repoWithBinayPlugin.GetAll()
			assert.Len(t, repoPlugins, 1)
		})
		t.Run("should not load yaml plugin for invalid paths or yaml", func(t *testing.T) {
			repo := models.NewPluginRepository()
			invalidPluginPaths := []string{
				"tests/notpresent.yaml",
				"tests/sample_plugin_without_version.yaml",
				"tests/sample_plugin_schema_invalid.yaml",
			}
			yaml.Init(repo, invalidPluginPaths, pluginLogger)
			assert.Empty(t, repo.GetAll())
		})
	})
}
