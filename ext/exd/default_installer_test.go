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
	defaultFS := exd.InstallerFS
	defer func() { exd.InstallerFS = defaultFS }()
	exd.InstallerFS = afero.NewMemMapFs()

	d.Run("should return error if remote metadata is nil", func() {
		var remoteMetadata *exd.RemoteMetadata
		installer := exd.NewDefaultInstaller()

		actualPrepareErr := installer.Prepare(remoteMetadata)

		d.Error(actualPrepareErr)
	})

	d.Run("should create directory", func() {
		dirPath := "./extension"
		remoteMetadata := &exd.RemoteMetadata{
			DirPath: dirPath,
		}
		installer := exd.NewDefaultInstaller()

		actualPrepareErr := installer.Prepare(remoteMetadata)
		actualInfo, actualStatErr := exd.InstallerFS.Stat(dirPath)

		d.NoError(actualPrepareErr)
		d.NoError(actualStatErr)
		d.True(actualInfo.IsDir())
	})
}

func (d *DefaultInstallerTestSuite) TestInstall() {
	defaultFS := exd.InstallerFS
	defer func() { exd.InstallerFS = defaultFS }()
	exd.InstallerFS = afero.NewMemMapFs()

	d.Run("should return error if asset is nil", func() {
		remoteMetadata := &exd.RemoteMetadata{
			DirPath: "./extension",
			TagName: "valor",
		}
		installer := exd.NewDefaultInstaller()

		var asset []byte

		actualInstallErr := installer.Install(asset, remoteMetadata)

		d.Error(actualInstallErr)
	})

	d.Run("should return error if remote metadata is nil", func() {
		var remoteMetadata *exd.RemoteMetadata
		installer := exd.NewDefaultInstaller()

		asset := []byte("lorem ipsum")

		actualInstallErr := installer.Install(asset, remoteMetadata)

		d.Error(actualInstallErr)
	})

	d.Run("should write asset to the targeted path", func() {
		dirPath := "./extension"
		tagName := "valor"
		remoteMetadata := &exd.RemoteMetadata{
			DirPath: dirPath,
			TagName: tagName,
		}
		installer := exd.NewDefaultInstaller()
		filePath := path.Join(dirPath, tagName)

		message := "lorem ipsum"
		asset := []byte(message)

		actualInstallErr := installer.Install(asset, remoteMetadata)
		defer d.removeDir(dirPath)
		actualFile, actualOpenErr := exd.InstallerFS.OpenFile(filePath, os.O_RDONLY, 0o755)
		actualContent, actualReadErr := io.ReadAll(actualFile)

		d.NoError(actualInstallErr)
		d.NoError(actualOpenErr)
		d.NoError(actualReadErr)
		d.Equal(message, string(actualContent))
	})
}

func (*DefaultInstallerTestSuite) removeDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestDefaultInstaller(t *testing.T) {
	suite.Run(t, &DefaultInstallerTestSuite{})
}
