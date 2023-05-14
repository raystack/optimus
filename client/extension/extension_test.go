package extension_test

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/client/extension"
	"github.com/odpf/optimus/client/extension/model"
)

func TestClean(t *testing.T) {
	defaultFS := extension.CleanExtensionFS
	defer func() { extension.CleanExtensionFS = defaultFS }()
	extension.CleanExtensionFS = afero.NewMemMapFs()

	defaultDir := model.ExtensionDir
	defer func() { model.ExtensionDir = defaultDir }()

	t.Run("should return nil if no error encountered during cleaning", func(t *testing.T) {
		verbose := true
		model.ExtensionDir = "extension"

		actualError := extension.Clean(verbose)

		assert.NoError(t, actualError)
	})
}
