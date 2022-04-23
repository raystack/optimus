package exd_test

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/exd"
)

type DefaultInstallerTestSuite struct {
	suite.Suite
}

func (d *DefaultInstallerTestSuite) TestPrepare() {
	defaultFS := exd.DefaultInstallerFS
	defer func() { exd.DefaultInstallerFS = defaultFS }()
	exd.DefaultInstallerFS = afero.NewMemMapFs()

	d.Run("should return error if metadata is nil", func() {
		var metadata *exd.Metadata
		installer := exd.NewDefaultInstaller()

		actualPrepareErr := installer.Prepare(metadata)

		d.Error(actualPrepareErr)
	})

	d.Run("should create directory", func() {
		dirPath := "./extension"
		metadata := &exd.Metadata{
			AssetDirPath: dirPath,
		}
		installer := exd.NewDefaultInstaller()

		actualPrepareErr := installer.Prepare(metadata)
		actualInfo, actualStatErr := exd.DefaultInstallerFS.Stat(dirPath)

		d.NoError(actualPrepareErr)
		d.NoError(actualStatErr)
		d.True(actualInfo.IsDir())
	})
}

func (d *DefaultInstallerTestSuite) TestInstall() {
	defaultFS := exd.DefaultInstallerFS
	defer func() { exd.DefaultInstallerFS = defaultFS }()
	exd.DefaultInstallerFS = afero.NewMemMapFs()

	d.Run("should return error if asset is nil", func() {
		var asset []byte
		dirPath := "./extension"
		metadata := &exd.Metadata{
			AssetDirPath: dirPath,
			TagName:      "valor",
		}
		installer := exd.NewDefaultInstaller()

		actualInstallErr := installer.Install(asset, metadata)

		d.Error(actualInstallErr)
	})

	d.Run("should return error if metadata is nil", func() {
		message := "lorem ipsum"
		asset := []byte(message)
		var metadata *exd.Metadata
		installer := exd.NewDefaultInstaller()

		actualInstallErr := installer.Install(asset, metadata)

		d.Error(actualInstallErr)
	})

	d.Run("should write asset to the targeted path", func() {
		message := "lorem ipsum"
		asset := []byte(message)
		dirPath := "./extension"
		tagName := "valor"
		metadata := &exd.Metadata{
			AssetDirPath: dirPath,
			TagName:      tagName,
		}
		installer := exd.NewDefaultInstaller()

		actualInstallErr := installer.Install(asset, metadata)
		defer d.removeDir(dirPath)
		filePath := path.Join(dirPath, tagName)
		actualFile, actualOpenErr := exd.DefaultInstallerFS.OpenFile(filePath, os.O_RDONLY, 0o755)
		actualContent, actualReadErr := io.ReadAll(actualFile)

		d.NoError(actualInstallErr)
		d.NoError(actualOpenErr)
		d.NoError(actualReadErr)
		d.Equal(message, string(actualContent))
	})
}

func (d *DefaultInstallerTestSuite) removeDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestDefaultInstaller(t *testing.T) {
	suite.Run(t, &DefaultInstallerTestSuite{})
}
