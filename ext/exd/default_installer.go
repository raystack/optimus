package exd

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/spf13/afero"
)

var DefaultInstallerFS = afero.NewOsFs()

type defaultInstaller struct {
}

func NewDefaultInstaller() Installer {
	return &defaultInstaller{}
}

func (d *defaultInstaller) Prepare(metadata *Metadata) error {
	if metadata == nil {
		return errors.New("metadata is nil")
	}
	directoryPermission := 0o750
	return DefaultInstallerFS.MkdirAll(metadata.AssetDirPath, fs.FileMode(directoryPermission))
}

func (d *defaultInstaller) Install(asset []byte, metadata *Metadata) error {
	if asset == nil {
		return errors.New("asset is nil")
	}
	if metadata == nil {
		return errors.New("metadata is nil")
	}
	filePath := path.Join(metadata.AssetDirPath, metadata.TagName)
	f, err := DefaultInstallerFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(asset)
	return err
}
