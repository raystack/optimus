package extension

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"

	"github.com/spf13/afero"

	"github.com/odpf/optimus/extension/model"
)

// AssetOperatorFS is file system that will be used by operator.
// It can be changed before calling any operation.
// But, make sure to change it back after the operation is done
// to its default value to avoid unexpected behaviour.
var AssetOperatorFS = afero.NewOsFs()

type defaultAssetOperator struct {
	localDirPath string

	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

// NewDefaultAssetOperator initializes default asset operator
func NewDefaultAssetOperator(stdin io.Reader, stdout io.Writer, stderr io.Writer) model.AssetOperator {
	return &defaultAssetOperator{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

func (d *defaultAssetOperator) Prepare(localDirPath string) error {
	d.localDirPath = localDirPath
	return nil
}

func (d *defaultAssetOperator) Install(asset []byte, tagName string) error {
	if asset == nil {
		return model.ErrNilAsset
	}
	directoryPermission := 0744
	if err := AssetOperatorFS.MkdirAll(d.localDirPath, fs.FileMode(directoryPermission)); err != nil {
		return fmt.Errorf("error making directory: %w", err)
	}
	filePath := path.Join(d.localDirPath, tagName)
	filePermission := 0755
	f, err := AssetOperatorFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fs.FileMode(filePermission))
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()
	_, err = f.Write(asset)
	return err
}

func (d *defaultAssetOperator) Uninstall(tagNames ...string) error {
	if len(tagNames) == 0 {
		if err := AssetOperatorFS.RemoveAll(d.localDirPath); err != nil {
			return fmt.Errorf("error removing directory: %w", err)
		}
		return nil
	}
	for _, name := range tagNames {
		filePath := path.Join(d.localDirPath, name)
		if err := AssetOperatorFS.RemoveAll(filePath); err != nil {
			return fmt.Errorf("error removing file: %w", err)
		}
	}
	return nil
}

func (d *defaultAssetOperator) Run(tagName string, args ...string) error {
	filePath := path.Join(d.localDirPath, tagName)
	cmd := exec.Command(filePath, args...)
	cmd.Stdin = d.stdin
	cmd.Stdout = d.stdout
	cmd.Stderr = d.stderr
	return cmd.Run()
}
