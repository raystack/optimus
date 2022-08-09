package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPluginModel(t *testing.T) {
	t.Run("test plugin model", func(t *testing.T) {
		t.Run("validate plugins questions", func(t *testing.T) {
			testQuest := PluginQuestion{
				Name:            "PROJECT",
				Regexp:          `^[a-z0-9_\-]+$`,
				ValidationError: "invalid name",
				MinLength:       3,
				MaxLength:       5,
			}
			assert.Error(t, testQuest.IsValid("ab"))
			assert.Error(t, testQuest.IsValid("abcdef"))
			assert.Error(t, testQuest.IsValid("ABCD"))
			assert.Empty(t, testQuest.IsValid("abc"))
		})
	})
}
