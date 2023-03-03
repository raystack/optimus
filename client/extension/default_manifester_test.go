package extension_test

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"

	"github.com/goto/optimus/client/extension"
	"github.com/goto/optimus/client/extension/model"
)

const (
	manifestFileName     = "manifest.yaml"
	manifestDirName      = "./valid-extension"
	manifestValidContent = `
updated_at: 2022-05-17T16:18:05.448219+07:00
repository_owners:
  - name: gojek
    projects:
    - name: optimus-extension-valor
      command_name: valor
      releases:
      - tag_name: v0.0.1
`
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

func (d *DefaultManifesterTestSuite) TestLoad() {
	defaultFS := extension.ManifesterFS
	defer func() { extension.ManifesterFS = defaultFS }()
	extension.ManifesterFS = afero.NewMemMapFs()

	d.Run("should return empty and nil if no file is found", func() {
		manifester := extension.NewDefaultManifester()

		actualManifest, actualErr := manifester.Load(manifestDirName)

		d.Empty(actualManifest)
		d.NoError(actualErr)
	})

	d.Run("should return nil and error if failed to unmarshal manifest", func() {
		d.writeFile(manifestDirName, manifestFileName, manifestInvalidContent)
		manifester := extension.NewDefaultManifester()

		actualManifest, actualErr := manifester.Load(manifestDirName)

		d.Nil(actualManifest)
		d.Error(actualErr)
	})

	d.Run("should return manifest and nil if no error encountered", func() {
		d.writeFile(manifestDirName, manifestFileName, manifestValidContent)
		manifester := extension.NewDefaultManifester()

		actualManifest, actualErr := manifester.Load(manifestDirName)

		d.NotNil(actualManifest)
		d.NoError(actualErr)
	})
}

func (d *DefaultManifesterTestSuite) TestFlush() {
	defaultFS := extension.ManifesterFS
	defer func() { extension.ManifesterFS = defaultFS }()
	extension.ManifesterFS = afero.NewMemMapFs()

	d.Run("should return error if manifest is nil", func() {
		var manifest *model.Manifest
		dirPath := "./extension"
		manifester := extension.NewDefaultManifester()

		actualErr := manifester.Flush(manifest, dirPath)

		d.Error(actualErr)
	})

	d.Run("should return nil and create file", func() {
		now := time.Now()
		manifest := &model.Manifest{
			UpdatedAt: now,
		}
		dirPath := "./extension"
		manifester := extension.NewDefaultManifester()

		actualErr := manifester.Flush(manifest, dirPath)
		filepath := path.Join(dirPath, "manifest.yaml")
		file, openErr := extension.ManifesterFS.OpenFile(filepath, os.O_RDONLY, 0o644)
		if openErr != nil {
			panic(openErr)
		}
		decoder := yaml.NewDecoder(file)
		var actualManifest model.Manifest
		unmarshallErr := decoder.Decode(&actualManifest)

		d.NoError(actualErr)
		d.NoError(unmarshallErr)
		d.True(actualManifest.UpdatedAt.Equal(manifest.UpdatedAt))
	})
}

func (*DefaultManifesterTestSuite) writeFile(dirPath, fileName, content string) {
	if err := extension.ManifesterFS.MkdirAll(dirPath, 0o744); err != nil {
		panic(err)
	}
	filePath := path.Join(dirPath, fileName)
	file, err := extension.ManifesterFS.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte(content)); err != nil {
		panic(err)
	}
}

func (*DefaultManifesterTestSuite) removeDir(dirPath string) {
	if err := extension.ManifesterFS.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}

func TestDefaultManifester(t *testing.T) {
	suite.Run(t, &DefaultManifesterTestSuite{})
}
