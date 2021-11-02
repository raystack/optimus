package extension

import (
	"fmt"
	"os"
	"path"
)

const extensionDirName = ".optimus/extensions"

func getExtensionDir() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("error getting user home dir: %v", err)
	}
	return path.Join(dir, extensionDirName), nil
}
