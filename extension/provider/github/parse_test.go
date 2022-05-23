package github_test

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/extension/model"
	"github.com/odpf/optimus/extension/provider/github"
)

func TestParse(t *testing.T) {
	t.Run("should return nil and error if remote path is empty", func(t *testing.T) {
		remotePath := ""

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.Nil(t, actualMetadata)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if remote path is not recognized", func(t *testing.T) {
		remotePath := "invalid-remote-path"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.Nil(t, actualMetadata)
		assert.ErrorIs(t, actualErr, model.ErrUnrecognizedRemotePath)
	})

	t.Run("should return remote metadata with provider github", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor"

		expectedProvider := "github"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedProvider, actualMetadata.ProviderName)
	})

	t.Run("should return remote metadata with the same owner name as expected", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor"

		expectedOwnername := "gojek"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedOwnername, actualMetadata.OwnerName)
	})

	t.Run("should return remote metadata with the same repo name as expected", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor"

		expectedRepoName := "optimus-extension-valor"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedRepoName, actualMetadata.ProjectName)
	})

	t.Run("should return remote metadata with empty tag name if not specified", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor"

		expectedTagName := ""

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedTagName, actualMetadata.TagName)
	})

	t.Run("should return remote metadata with the same tag name as expected", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor@v1.0.0"

		expectedTagName := "v1.0.0"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedTagName, actualMetadata.TagName)
	})

	t.Run("should return remote metadata with empty current api path but latest upgrade api path if tag is not specified", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor"

		expectedUpgradeAPIPath := "https://api.github.com/repos/gojek/optimus-extension-valor/releases/latest"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Empty(t, actualMetadata.CurrentAPIPath)
		assert.Equal(t, expectedUpgradeAPIPath, actualMetadata.UpgradeAPIPath)
	})

	t.Run("should return remote metadata with current api path and latest upgrade api path if tag is specified", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor@v1.0.0"

		expectedCurrentAPIPath := "https://api.github.com/repos/gojek/optimus-extension-valor/releases/tags/v1.0.0"
		expectedUpgradeAPIPath := "https://api.github.com/repos/gojek/optimus-extension-valor/releases/latest"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedCurrentAPIPath, actualMetadata.CurrentAPIPath)
		assert.Equal(t, expectedUpgradeAPIPath, actualMetadata.UpgradeAPIPath)
	})

	t.Run("should return remote metadata with the same dir path as expected", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor@v1.0.0"

		homeDir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		expectedAssetDirPath := path.Join(homeDir, ".optimus", "extensions", "github.com", "gojek", "optimus-extension-valor")

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedAssetDirPath, actualMetadata.LocalDirPath)
	})

	t.Run("should return remote metadata with the same command name as expected", func(t *testing.T) {
		remotePath := "github.com/gojek/optimus-extension-valor@v1.0.0"

		expectedCommandName := "valor"

		actualMetadata, actualErr := github.Parse(remotePath)

		assert.NotNil(t, actualMetadata)
		assert.NoError(t, actualErr)
		assert.Equal(t, expectedCommandName, actualMetadata.CommandName)
	})
}
