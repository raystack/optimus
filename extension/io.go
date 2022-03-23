package extension

import (
	"fmt"
	"os"
	"path"
)

const extensionDirName = ".optimus/extensions"

// GetDefaultDir gets the default extension directory
func GetDefaultDir() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("error getting user home dir: %w", err)
	}
	return path.Join(dir, extensionDirName), nil
}
