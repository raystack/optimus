package exd_test

import (
	"os"
	"path"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/exd"
)

const (
	manifestFileName       = "manifest.yaml"
	manifestDirName        = "./valid-extension"
	manifestValidContent   = "updated_at: 2021-11-02T17:06:58.624013+07:00"
	manifestInvalidContent = "updated_at: invalid"
)

type DefaultManifesterTestSuite struct {
	suite.Suite
}

func (d *DefaultManifesterTestSuite) SetupTest() {
	d.removeDir(manifestDirName)
}

func (d *DefaultManifesterTestSuite) TearDownTest() {
	d.removeDir(manifestDirName)
}

func (d *DefaultManifesterTestSuite) TestLoadManifest() {
	defaultFS := exd.ManifesterFS
	defer func() { exd.ManifesterFS = defaultFS }()
	exd.ManifesterFS = afero.NewMemMapFs()

	d.Run("should return empty and nil if no file is found", func() {
		manifester := exd.NewDefaultManifester()

		actualManifest, actualErr := manifester.Load(manifestDirName)

		d.Empty(actualManifest)
		d.NoError(actualErr)
	})

	d.Run("should return nil and error if failed to unmarshal manifest", func() {
		d.writeFile(manifestDirName, manifestFileName, manifestInvalidContent)
		manifester := exd.NewDefaultManifester()

		actualManifest, actualErr := manifester.Load(manifestDirName)

		d.Nil(actualManifest)
		d.Error(actualErr)
	})

	d.Run("should return manifest and nil if no error encountered", func() {
		d.writeFile(manifestDirName, manifestFileName, manifestValidContent)
		manifester := exd.NewDefaultManifester()

		actualManifest, actualErr := manifester.Load(manifestDirName)

		d.NotNil(actualManifest)
		d.NoError(actualErr)
	})
}

func (d *DefaultManifesterTestSuite) writeFile(dirPath, fileName, content string) {
	if err := exd.ManifesterFS.MkdirAll(dirPath, 0o755); err != nil {
		panic(err)
	}
	filePath := path.Join(dirPath, fileName)
	file, err := exd.ManifesterFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte(content)); err != nil {
		panic(err)
	}
}

func (d *DefaultManifesterTestSuite) removeDir(dirPath string) {
	if err := exd.ManifesterFS.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestDefaultManifester(t *testing.T) {
	suite.Run(t, &DefaultManifesterTestSuite{})
}
