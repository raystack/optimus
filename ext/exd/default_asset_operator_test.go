package exd_test

import (
	"io"
	"io/fs"
	"os"
	"path"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/exd"
)

type DefaultAssetOperatorTestSuite struct {
	suite.Suite
}

func (d *DefaultAssetOperatorTestSuite) TestPrepare() {
	d.Run("should return no error", func() {
		localDirPath := "./extension"
		assetOperator := exd.NewDefaultAssetOperator(nil, nil, nil)

		actualErr := assetOperator.Prepare(localDirPath)

		d.NoError(actualErr)
	})
}

func (d *DefaultAssetOperatorTestSuite) TestInstall() {
	defaultFS := exd.AssetOperatorFS
	defer func() { exd.AssetOperatorFS = defaultFS }()
	exd.AssetOperatorFS = afero.NewMemMapFs()

	localDirPath := "./extension"
	d.Run("should return error if asset is nil", func() {
		tagName := "v1.0.0"
		assetOperator := exd.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)

		var asset []byte

		actualInstallErr := assetOperator.Install(asset, tagName)

		d.Error(actualInstallErr)
	})

	d.Run("should write asset to the targeted path", func() {
		tagName := "valor"
		assetOperator := exd.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)
		filePath := path.Join(localDirPath, tagName)

		message := "lorem ipsum"
		asset := []byte(message)

		actualInstallErr := assetOperator.Install(asset, tagName)
		defer d.removeDir(localDirPath)
		actualFile, actualOpenErr := exd.AssetOperatorFS.OpenFile(filePath, os.O_RDONLY, 0o755)
		actualContent, actualReadErr := io.ReadAll(actualFile)

		d.NoError(actualInstallErr)
		d.NoError(actualOpenErr)
		d.NoError(actualReadErr)
		d.Equal(message, string(actualContent))
	})
}

func (d *DefaultAssetOperatorTestSuite) TestUninstall() {
	defaultFS := exd.AssetOperatorFS
	defer func() { exd.AssetOperatorFS = defaultFS }()
	exd.AssetOperatorFS = afero.NewMemMapFs()

	localDirPath := "./extension"
	d.Run("should delete directory if no file names specified and return nil", func() {
		directoryPermission := 0o750
		if err := exd.AssetOperatorFS.MkdirAll(localDirPath, fs.FileMode(directoryPermission)); err != nil {
			panic(err)
		}
		assetOperator := exd.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)

		actualErr := assetOperator.Uninstall()
		_, statErr := exd.AssetOperatorFS.Stat(localDirPath)

		d.NoError(actualErr)
		d.ErrorIs(statErr, os.ErrNotExist)
	})

	d.Run("should delete files only if specified and return nil", func() {
		directoryPermission := 0o750
		if err := exd.AssetOperatorFS.MkdirAll(localDirPath, fs.FileMode(directoryPermission)); err != nil {
			panic(err)
		}
		message := "lorem ipsum"
		asset := []byte(message)
		fileName := "asset"
		filePath := path.Join(localDirPath, fileName)
		filePermission := 0o755
		file, err := exd.AssetOperatorFS.OpenFile(filePath, os.O_CREATE, fs.FileMode(filePermission))
		if err != nil {
			panic(err)
		}
		if _, err := file.Write(asset); err != nil {
			panic(err)
		}

		assetOperator := exd.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)

		actualErr := assetOperator.Uninstall(fileName)
		dirStat, dirStatErr := exd.AssetOperatorFS.Stat(localDirPath)
		_, fileStatErr := exd.AssetOperatorFS.Stat(filePath)

		d.NoError(actualErr)
		d.True(dirStat.IsDir())
		d.NoError(dirStatErr)
		d.ErrorIs(fileStatErr, os.ErrNotExist)
	})
}

func (d *DefaultAssetOperatorTestSuite) TestRun() {
	defaultFS := exd.AssetOperatorFS
	defer func() { exd.AssetOperatorFS = defaultFS }()
	exd.AssetOperatorFS = afero.NewMemMapFs()

	d.Run("should return the result from executing run", func() {
		dirPath := "./extension"
		tagName := "v1.0"
		assetOperator := exd.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(dirPath)

		actualResult := assetOperator.Run(tagName)

		d.Error(actualResult)
	})
}

func (*DefaultAssetOperatorTestSuite) removeDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestDefaultAssetOperator(t *testing.T) {
	suite.Run(t, &DefaultAssetOperatorTestSuite{})
}
