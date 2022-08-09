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

func TestPlugin(t *testing.T) {
	var testPluginPaths = []string{"tests/bq2bqtest.yaml"}
	t.Run("load", func(t *testing.T) {
		testQuest := models.PluginQuestion{
			Name:            "PROJECT",
			Regexp:          `^[a-z0-9_\-]+$`,
			ValidationError: "invalid name",
			MinLength:       3,
			MaxLength:       5,
		}

		t.Run("init yaml plugins spec", func(t *testing.T) {
			// repo := new(mock.SupportedPluginRepo)
			repo := models.NewPluginRepository()
			pluginPaths := testPluginPaths
			pluginLogger := hclog.New(&hclog.LoggerOptions{
				Name:   "optimus",
				Output: os.Stdout,
			})
			yaml.Init(repo, pluginPaths, pluginLogger)
			assert.NotEmpty(t, repo.GetAll())
		})
		t.Run("load plugin spec", func(t *testing.T) {
			plugin, err := yaml.NewPlugin(testPluginPaths[0])
			assert.Equal(t, nil, err)
			assert.NotEqual(t, nil, plugin)

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
			info, _ := plugin.PluginInfo()
			assert.Equal(t, expectedInfo, info)

			ctx := context.Background()

			questReq := models.GetQuestionsRequest{JobName: "test"}
			actualQuestions, err := plugin.GetQuestions(ctx, questReq)
			expectedQuestions := models.GetQuestionsResponse{
				Questions: models.PluginQuestions{},
			}
			assert.Equal(t, nil, err)
			assert.NotEqual(t, expectedQuestions, actualQuestions)

			ctx = context.Background()
			configReq := models.DefaultConfigRequest{
				Answers: models.PluginAnswers{
					models.PluginAnswer{
						Question: testQuest,
						Value:    "test_project",
					},
				},
			}
			actualConfig, err := plugin.DefaultConfig(ctx, configReq)
			expectedConfig := models.DefaultConfigResponse{
				Config: models.PluginConfigs{},
			}
			assert.Equal(t, nil, err)
			assert.NotEqual(t, expectedConfig, actualConfig)

			ctx = context.Background()
			assetReq := models.DefaultAssetsRequest{}
			actualAssets, err := plugin.DefaultAssets(ctx, assetReq)
			expectedAssets := models.DefaultAssetsResponse{
				Assets: models.PluginAssets{},
			}
			assert.Equal(t, nil, err)
			assert.NotEqual(t, expectedAssets, actualAssets)
		})
	})
}
