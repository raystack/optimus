package exd

import (
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/spf13/afero"
)

// InstallerFS is file system that will be used by installer.
// It can be changed before calling any installation operation.
// But, make sure to change it back after the operation is done
// to its default value to avoid unexpected behaviour.
var InstallerFS = afero.NewOsFs()

type defaultInstaller struct {
}

// NewDefaultInstaller initializes default installer
func NewDefaultInstaller() Installer {
	return &defaultInstaller{}
}

// Prepare prepares installation based on the remote metadata
func (*defaultInstaller) Prepare(dirPath string) error {
	directoryPermission := 0o750
	return InstallerFS.MkdirAll(dirPath, fs.FileMode(directoryPermission))
}

// Install installs asset based on the remote metadata
func (*defaultInstaller) Install(asset []byte, dirPath, fileName string) error {
	if asset == nil {
		return ErrNilAsset
	}
	filePath := path.Join(dirPath, fileName)
	f, err := InstallerFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(asset)
	return err
}
