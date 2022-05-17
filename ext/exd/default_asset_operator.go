package exd

import (
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/spf13/afero"
)

// AssetOperatorFS is file system that will be used by operator.
// It can be changed before calling any operation.
// But, make sure to change it back after the operation is done
// to its default value to avoid unexpected behaviour.
var AssetOperatorFS = afero.NewOsFs()

type defaultAssetOperator struct {
	dirPath string
}

// NewDefaultAssetOperator initializes default asset operator
func NewDefaultAssetOperator() AssetOperator {
	return &defaultAssetOperator{}
}

func (d *defaultAssetOperator) Prepare(dirPath string) error {
	d.dirPath = dirPath
	return nil
}

func (d *defaultAssetOperator) Install(asset []byte, fileName string) error {
	if asset == nil {
		return ErrNilAsset
	}
	directoryPermission := 0o750
	if err := AssetOperatorFS.MkdirAll(d.dirPath, fs.FileMode(directoryPermission)); err != nil {
		return fmt.Errorf("error making directory: %w", err)
	}
	filePath := path.Join(d.dirPath, fileName)
	filePermission := 0o755
	f, err := AssetOperatorFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(filePermission))
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(asset)
	return err
}

func (d *defaultAssetOperator) Uninstall(fileNames ...string) error {
	if len(fileNames) == 0 {
		if err := AssetOperatorFS.RemoveAll(d.dirPath); err != nil {
			return fmt.Errorf("error removing directory: %w", err)
		}
		return nil
	}
	for _, name := range fileNames {
		filePath := path.Join(d.dirPath, name)
		if err := AssetOperatorFS.RemoveAll(filePath); err != nil {
			return fmt.Errorf("error removing file: %w", err)
		}
	}
	return nil
}
