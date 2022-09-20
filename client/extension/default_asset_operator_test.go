package extension_test

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/client/extension"
)

type DefaultAssetOperatorTestSuite struct {
	suite.Suite
}

func (d *DefaultAssetOperatorTestSuite) TestPrepare() {
	d.Run("should return no error", func() {
		localDirPath := "./extension"
		assetOperator := extension.NewDefaultAssetOperator(nil, nil, nil)

		actualErr := assetOperator.Prepare(localDirPath)

		d.NoError(actualErr)
	})
}

func (d *DefaultAssetOperatorTestSuite) TestInstall() {
	defaultFS := extension.AssetOperatorFS
	defer func() { extension.AssetOperatorFS = defaultFS }()
	extension.AssetOperatorFS = afero.NewMemMapFs()

	localDirPath := "./extension"
	d.Run("should return error if asset is nil", func() {
		tagName := "v1.0.0"
		assetOperator := extension.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)

		var asset []byte

		actualInstallErr := assetOperator.Install(asset, tagName)

		d.Error(actualInstallErr)
	})

	d.Run("should write asset to the targeted path", func() {
		tagName := "valor"
		assetOperator := extension.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)
		filePath := path.Join(localDirPath, tagName)

		message := "lorem ipsum"
		asset := []byte(message)

		actualInstallErr := assetOperator.Install(asset, tagName)

		actualFile, actualOpenErr := extension.AssetOperatorFS.OpenFile(filePath, os.O_RDONLY, 0755)
		actualContent, actualReadErr := io.ReadAll(actualFile)

		d.NoError(actualInstallErr)
		d.NoError(actualOpenErr)
		d.NoError(actualReadErr)
		d.Equal(message, string(actualContent))
	})
}

func (d *DefaultAssetOperatorTestSuite) TestUninstall() {
	defaultFS := extension.AssetOperatorFS
	defer func() { extension.AssetOperatorFS = defaultFS }()
	extension.AssetOperatorFS = afero.NewMemMapFs()

	localDirPath := "./extension"
	d.Run("should delete directory if no file names specified and return nil", func() {
		if err := extension.AssetOperatorFS.MkdirAll(localDirPath, 0744); err != nil {
			panic(err)
		}
		assetOperator := extension.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)

		actualErr := assetOperator.Uninstall()
		_, statErr := extension.AssetOperatorFS.Stat(localDirPath)

		d.NoError(actualErr)
		d.ErrorIs(statErr, os.ErrNotExist)
	})

	d.Run("should delete files only if specified and return nil", func() {
		if err := extension.AssetOperatorFS.MkdirAll(localDirPath, 0744); err != nil {
			panic(err)
		}
		message := "lorem ipsum"
		asset := []byte(message)
		fileName := "asset"
		filePath := path.Join(localDirPath, fileName)
		file, err := extension.AssetOperatorFS.OpenFile(filePath, os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		if _, err := file.Write(asset); err != nil {
			panic(err)
		}

		assetOperator := extension.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(localDirPath)

		actualErr := assetOperator.Uninstall(fileName)
		dirStat, dirStatErr := extension.AssetOperatorFS.Stat(localDirPath)
		_, fileStatErr := extension.AssetOperatorFS.Stat(filePath)

		d.NoError(actualErr)
		d.True(dirStat.IsDir())
		d.NoError(dirStatErr)
		d.ErrorIs(fileStatErr, os.ErrNotExist)
	})
}

func (d *DefaultAssetOperatorTestSuite) TestRun() {
	defaultFS := extension.AssetOperatorFS
	defer func() { extension.AssetOperatorFS = defaultFS }()
	extension.AssetOperatorFS = afero.NewMemMapFs()

	d.Run("should return the result from executing run", func() {
		dirPath := "./extension"
		tagName := "v1.0"
		assetOperator := extension.NewDefaultAssetOperator(nil, nil, nil)
		assetOperator.Prepare(dirPath)

		actualResult := assetOperator.Run(tagName)

		d.Error(actualResult)
	})
}

func TestDefaultAssetOperator(t *testing.T) {
	suite.Run(t, &DefaultAssetOperatorTestSuite{})
}
