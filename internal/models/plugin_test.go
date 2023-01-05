package models_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/sdk/plugin"
	mockPlugin "github.com/odpf/optimus/sdk/plugin/mock"
)

func TestPluginModels(t *testing.T) {
	t.Run("PluginRegistry", func(t *testing.T) {
		repo := models.NewPluginRepository()
		plugins := map[string]*plugin.Plugin{
			"c": mockPlugin.NewMockYamlPlugin("c", plugin.TypeTask.String()),
			"b": mockPlugin.NewMockYamlPlugin("b", plugin.TypeTask.String()),
			"z": mockPlugin.NewMockYamlPlugin("z", plugin.TypeTask.String()),
			"a": mockPlugin.NewMockBinaryPlugin("a", plugin.TypeHook.String()),
		}
		for _, p := range plugins {
			repo.AddYaml(p.YamlMod)
			if p.DependencyMod != nil {
				repo.AddBinary(p.DependencyMod)
			}
		}
		t.Run("should allow both yaml and bin implementations in plugin", func(t *testing.T) {
			yamlPlugin, _ := repo.GetByName("a")
			assert.Equal(t, yamlPlugin.IsYamlPlugin(), true)
			assert.NotNil(t, yamlPlugin.YamlMod)
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
