package plugin_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/sdk/plugin"
	"github.com/odpf/optimus/sdk/plugin/mock"
)

func TestPlugins(t *testing.T) {
	t.Run("Plugin", func(t *testing.T) {
		t.Run("IsYamlPlugin", func(t *testing.T) {
			binaryTask := mock.NewMockBinaryPlugin("abc", plugin.TypeTask.String())
			assert.True(t, binaryTask.IsYamlPlugin())

			yamlTask := mock.NewMockYamlPlugin("abc", plugin.TypeTask.String())
			assert.True(t, yamlTask.IsYamlPlugin())
		})
		t.Run("GetSurveyMethod", func(t *testing.T) {
			binaryTask := mock.NewMockBinaryPlugin("abc", plugin.TypeTask.String())
			assert.Equal(t, binaryTask.YamlMod, binaryTask.GetSurveyMod())

			yamlTask := mock.NewMockYamlPlugin("abc", plugin.TypeTask.String())
			assert.Equal(t, yamlTask.YamlMod, yamlTask.GetSurveyMod())
		})
		t.Run("Info", func(t *testing.T) {
			binaryPlugin := mock.NewMockBinaryPlugin("abc", plugin.TypeTask.String())
			assert.Equal(t, "abc", binaryPlugin.Info().Name)

			yamlPlugin := mock.NewMockYamlPlugin("abcd", plugin.TypeTask.String())
			assert.Equal(t, "abcd", yamlPlugin.Info().Name)
		})
	})

	t.Run("Info", func(t *testing.T) {
		t.Run("Validate", func(t *testing.T) {
			testCases := []struct {
				name string
				err  error
				info plugin.Info
			}{
				{
					name: "when name is empty",
					err:  errors.New("plugin name cannot be empty"),
					info: plugin.Info{
						Name:          "",
						Image:         "odpf.io/example",
						PluginVersion: "0.2",
						Entrypoint:    "sleep 10",
						PluginType:    plugin.TypeTask,
					},
				},
				{
					name: "when image is empty",
					err:  errors.New("plugin image cannot be empty"),
					info: plugin.Info{
						Name:          "example",
						Image:         "",
						PluginVersion: "0.2",
						Entrypoint:    "sleep 10",
						PluginType:    plugin.TypeTask,
					},
				},
				{
					name: "when plugin version is empty",
					err:  errors.New("plugin version cannot be empty"),
					info: plugin.Info{
						Name:          "example",
						Image:         "odpf.io/example",
						PluginVersion: "",
						Entrypoint:    "sleep 10",
						PluginType:    plugin.TypeTask,
					},
				},
				{
					name: "when entrypoint is empty",
					err:  errors.New("entrypoint cannot be empty"),
					info: plugin.Info{
						Name:          "example",
						Image:         "odpf.io/example",
						PluginVersion: "0.2",
						Entrypoint:    "",
						PluginType:    plugin.TypeTask,
					},
				},
				{
					name: "when plugin type is not supported",
					err:  errors.New("plugin type is not supported"),
					info: plugin.Info{
						Name:          "example",
						Image:         "odpf.io/example",
						PluginVersion: "0.2",
						Entrypoint:    "sleep 10",
						PluginType:    "",
					},
				},
				{
					name: "when valid",
					err:  nil,
					info: plugin.Info{
						Name:          "example",
						Image:         "odpf.io/example",
						PluginVersion: "0.2",
						Entrypoint:    "sleep 10",
						PluginType:    plugin.TypeTask,
					},
				},
			}

			for _, tc := range testCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					err := tc.info.Validate()
					assert.Equal(t, tc.err, err)
				})
			}
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
