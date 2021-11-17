package extension_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/odpf/optimus/extension"

	"github.com/stretchr/testify/assert"
)

func TestLoadManifest(t *testing.T) {
	const (
		manifestFileName       = "manifest.yaml"
		extensionDir           = "./valid-extension"
		validManifestContent   = "update: 2021-11-02T17:06:58.624013+07:00"
		invalidManifestContent = "update: invalid"
	)

	removeDir(extensionDir)

	t.Run("should return empty and nil if no file is found", func(t *testing.T) {
		manifest, err := extension.LoadManifest(extensionDir)

		assert.Empty(t, manifest)
		assert.NoError(t, err)
	})

	t.Run("should return nil and error if failed to unmarshal manifest", func(t *testing.T) {
		writeFile(extensionDir, manifestFileName, invalidManifestContent)

		manifest, err := extension.LoadManifest(extensionDir)

		assert.Nil(t, manifest)
		assert.Error(t, err)
	})

	t.Run("should return manifest and nil if no error encountered", func(t *testing.T) {
		writeFile(extensionDir, manifestFileName, validManifestContent)

		manifest, err := extension.LoadManifest(extensionDir)

		assert.NotNil(t, manifest)
		assert.NoError(t, err)
	})

	removeDir(extensionDir)
}

func writeFile(dirPath, fileName, content string) {
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		panic(err)
	}
	filePath := path.Join(dirPath, fileName)
	if err := ioutil.WriteFile(filePath, []byte(content), os.ModePerm); err != nil {
		panic(err)
	}
}

func removeDir(dirPath string) {
	if err := os.RemoveAll(dirPath); err != nil {
		panic(err)
	}
}
