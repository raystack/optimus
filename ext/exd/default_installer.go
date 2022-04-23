package exd

import (
	"errors"
	"io/fs"
	"os"
	"path"
)

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
	return os.MkdirAll(metadata.AssetDirPath, fs.FileMode(directoryPermission))
}

func (d *defaultInstaller) Install(asset []byte, metadata *Metadata) error {
	if asset == nil {
		return errors.New("asset is nil")
	}
	if metadata == nil {
		return errors.New("metadata is nil")
	}
	filePath := path.Join(metadata.AssetDirPath, metadata.TagName)
	return os.WriteFile(filePath, asset, 0o755)
}
