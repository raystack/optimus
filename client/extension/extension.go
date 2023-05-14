package extension

import (
	"github.com/spf13/afero"

	"github.com/odpf/optimus/client/extension/internal"
	"github.com/odpf/optimus/client/extension/model"
)

// CleanExtensionFS is file system that will be used when cleaning extension.
// It can be changed before calling the clean operation.
// But, make sure to change it back after the operation is done
// to its default value to avoid unexpected behaviour.
var CleanExtensionFS = afero.NewOsFs()

// Clean cleans all extensions from local, including its manifest
func Clean(verbose bool) error {
	if err := CleanExtensionFS.RemoveAll(model.ExtensionDir); err != nil {
		return internal.FormatError(verbose, err, "error encountered when cleaning extension directory")
	}
	return nil
}
