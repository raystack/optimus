package exd

import (
	"errors"
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

// Prepare prepares installation based on the metadata
func (d *defaultInstaller) Prepare(metadata *Metadata) error {
	if metadata == nil {
		return errors.New("metadata is nil")
	}
	directoryPermission := 0o750
	return InstallerFS.MkdirAll(metadata.AssetDirPath, fs.FileMode(directoryPermission))
}

// Install installs asset based on the metadata
func (d *defaultInstaller) Install(asset []byte, metadata *Metadata) error {
	if asset == nil {
		return errors.New("asset is nil")
	}
	if metadata == nil {
		return errors.New("metadata is nil")
	}
	filePath := path.Join(metadata.AssetDirPath, metadata.TagName)
	f, err := InstallerFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(asset)
	return err
}
