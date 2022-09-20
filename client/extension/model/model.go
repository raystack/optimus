package model

import (
	"os"
	"path"
)

// ExtensionDir is directory path where to store the extensions
var ExtensionDir string

func init() { //nolint:gochecknoinits
	userHomeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	ExtensionDir = path.Join(userHomeDir, ".optimus/extensions")
}
