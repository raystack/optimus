package exd_test

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/exd"
)

type DefaultInstallerTestSuite struct {
	suite.Suite
}

func (d *DefaultInstallerTestSuite) TestPrepare() {
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
		actualInfo, actualStatErr := os.Stat(dirPath)

		d.NoError(actualPrepareErr)
		d.NoError(actualStatErr)
		d.True(actualInfo.IsDir())
	})
}

func (d *DefaultInstallerTestSuite) TestInstall() {
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
		actualFile, actualFileErr := os.ReadFile(path.Join(dirPath, tagName))

		d.NoError(actualInstallErr)
		d.NoError(actualFileErr)
		d.Equal(message, string(actualFile))
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
