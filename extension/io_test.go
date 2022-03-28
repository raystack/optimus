package extension_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/extension"
)

func TestGetDefaultDir(t *testing.T) {
	t.Run("should return complete extension dir and nil if no error is encountered", func(t *testing.T) {
		expectedSuffix := ".optimus/extensions"

		actualValue, actualErr := extension.GetDefaultDir()

		assert.Contains(t, actualValue, expectedSuffix)
		assert.NoError(t, actualErr)
	})
}
