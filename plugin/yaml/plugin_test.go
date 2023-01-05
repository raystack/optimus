package yaml_test

import (
	"context"
	"os"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/plugin/yaml"
	"github.com/odpf/optimus/sdk/plugin"
)

type mockYamlMod struct {
	Name          string
	Image         string
	PluginVersion string
	PluginType    string
}

func (p *mockYamlMod) PluginInfo() *plugin.Info {
	return &plugin.Info{
		Name:          p.Name,
		Image:         p.Image,
		PluginVersion: p.PluginVersion,
		PluginType:    plugin.Type(p.PluginType),
	}
}

func (*mockYamlMod) GetQuestions(context.Context, plugin.GetQuestionsRequest) (*plugin.GetQuestionsResponse, error) {
	return &plugin.GetQuestionsResponse{Questions: plugin.Questions{}}, nil
}

func (*mockYamlMod) ValidateQuestion(context.Context, plugin.ValidateQuestionRequest) (*plugin.ValidateQuestionResponse, error) {
	return &plugin.ValidateQuestionResponse{Success: true}, nil
}

func (*mockYamlMod) DefaultConfig(context.Context, plugin.DefaultConfigRequest) (*plugin.DefaultConfigResponse, error) {
	return &plugin.DefaultConfigResponse{Config: plugin.Configs{}}, nil
}

func (*mockYamlMod) DefaultAssets(context.Context, plugin.DefaultAssetsRequest) (*plugin.DefaultAssetsResponse, error) {
	return &plugin.DefaultAssetsResponse{Assets: plugin.Assets{}}, nil
}

func TestYamlPlugin(t *testing.T) {
	testYamlPluginPath := "tests/sample_plugin.yaml" // success
	testYamlPluginName := "bq2bqtest"
	expectedInfo := &plugin.Info{
		Name:          "bq2bqtest",
		Description:   "Testing",
		Image:         "docker.io/odpf/optimus-task-bq2bq-executor:latest",
		PluginType:    "task",
		PluginMods:    []plugin.Mod{"cli"},
		PluginVersion: "latest",
		HookType:      "",
		DependsOn:     []string(nil),
		APIVersion:    []string(nil),
	}
	testPluginQuestion := plugin.Question{
		Name:      "PROJECT",
		Prompt:    "Project ID",
		Regexp:    `^[a-zA-Z0-9_\-]+$`,
		MinLength: 3,
	}
	expectedQuestions := &plugin.GetQuestionsResponse{
		Questions: plugin.Questions{
			testPluginQuestion,
		},
	}
	expectedAssets := &plugin.DefaultAssetsResponse{
		Assets: plugin.Assets{
			plugin.Asset{
				Name:  "query.sql",
				Value: `Select * from "project.dataset.table";`,
			},
		},
	}

	t.Run("PluginSpec", func(t *testing.T) {
		yamlPlugin, _ := yaml.NewPluginSpec(testYamlPluginPath)
		t.Run("PluginInfo", func(t *testing.T) {
			actual := yamlPlugin.PluginInfo()
			assert.Equal(t, expectedInfo, actual)
		})
		t.Run("GetQuestions", func(t *testing.T) {
			ctx := context.Background()
			questReq := plugin.GetQuestionsRequest{JobName: "test"}
			actual, err := yamlPlugin.GetQuestions(ctx, questReq)
			assert.Nil(t, err)
			assert.Equal(t, expectedQuestions, actual)
		})

		t.Run("ValidateQuestionSuccess", func(t *testing.T) {
			ctx := context.Background()
			req := plugin.ValidateQuestionRequest{
				Answer: plugin.Answer{
					Question: testPluginQuestion,
					Value:    "test_project",
				},
			}
			actual, err := yamlPlugin.ValidateQuestion(ctx, req)
			expected := plugin.ValidateQuestionResponse{Success: true}
			assert.Nil(t, err)
			assert.Equal(t, expected.Success, actual.Success)
		})
		t.Run("ValidateQuestionFailure", func(t *testing.T) {
			ctx := context.Background()
			req := plugin.ValidateQuestionRequest{
				Answer: plugin.Answer{
					Question: testPluginQuestion,
					Value:    "",
				},
			}
			actual, err := yamlPlugin.ValidateQuestion(ctx, req)
			expected := plugin.ValidateQuestionResponse{Success: false}
			assert.Nil(t, err)
			assert.Equal(t, expected.Success, actual.Success)
		})
		t.Run("DefaultConfig", func(t *testing.T) {
			ctx := context.Background()
			req := plugin.DefaultConfigRequest{
				Answers: plugin.Answers{
					plugin.Answer{
						Question: plugin.Question{Name: "PROJECT"},
						Value:    "test_project",
					},
				},
			}
			actual, err := yamlPlugin.DefaultConfig(ctx, req)
			expected := &plugin.DefaultConfigResponse{
				Config: plugin.Configs{
					plugin.Config{
						Name:  "PROJECT",
						Value: "test_project",
					},
					plugin.Config{
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
			req := plugin.DefaultAssetsRequest{}
			actual, err := yamlPlugin.DefaultAssets(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, expectedAssets, actual)
		})
	})

	t.Run("PluginsInitialization", func(t *testing.T) {
		pluginLogger := hclog.New(&hclog.LoggerOptions{
			Name:   "optimus",
			Output: os.Stdout,
		})
		t.Run("should load plugin for valid paths", func(t *testing.T) {
			repo := models.NewPluginRepository()
			err := yaml.Init(repo, []string{testYamlPluginPath}, pluginLogger)
			assert.NoError(t, err)
			assert.NotEmpty(t, repo.GetAll())
		})
		t.Run("should returns error when load yaml when same name exists", func(t *testing.T) {
			repoWithBinayPlugin := models.NewPluginRepository()
			err := repoWithBinayPlugin.AddYaml(&mockYamlMod{
				Name:          testYamlPluginName,
				Image:         "sdsd",
				PluginVersion: "asdasd",
				PluginType:    plugin.TypeTask.String(),
			})
			assert.Nil(t, err)
			assert.Len(t, repoWithBinayPlugin.GetAll(), 1)

			err = yaml.Init(repoWithBinayPlugin, []string{testYamlPluginPath}, pluginLogger)
			assert.Error(t, err)
			repoPlugins := repoWithBinayPlugin.GetAll()

			assert.Len(t, repoPlugins, 1)
			assert.Equal(t, repoPlugins[0].Info().Name, testYamlPluginName)
			assert.NotNil(t, repoPlugins[0].YamlMod)
		})
		t.Run("should not load duplicate yaml", func(t *testing.T) {
			repoWithBinayPlugin := models.NewPluginRepository()
			err := yaml.Init(repoWithBinayPlugin, []string{testYamlPluginPath, testYamlPluginPath}, pluginLogger)
			assert.Error(t, err)

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
			err := yaml.Init(repo, invalidPluginPaths, pluginLogger)
			assert.Error(t, err)
			assert.Empty(t, repo.GetAll())
		})
	})
}
