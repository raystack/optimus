package extension_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"testing"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/mock"

	"github.com/google/go-github/github"
	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ExtensionSuite struct {
	suite.Suite

	manifest         *extension.Manifest
	ghReleaseGetter  extension.GithubReleaseGetter
	httpDoer         extension.HTTPDoer
	dirPath          string
	reservedCommands []string

	validOwner    string
	invalidOwner  string
	validRepo     string
	invalidRepo   string
	conflictRepo  string
	validAlias    string
	conflictAlias string
}

func (e *ExtensionSuite) SetupSuite() {
	e.manifest = &extension.Manifest{}
	e.ghReleaseGetter = &mock.GithubReleaseGetter{}
	e.httpDoer = &mock.HTTPDoer{}
	e.dirPath = "./out"
	e.reservedCommands = []string{"help"}

	e.validOwner = "gojek"
	e.invalidOwner = ""
	e.validRepo = "optimus-extension-valor"
	e.invalidRepo = "valor"
	e.conflictRepo = "optimus-extension-help"
	e.validAlias = "validate"
	e.conflictAlias = "help"
}

func (e *ExtensionSuite) TestInstall() {
	e.Run("should return error if context is nil", func() {
		ext, err := extension.NewExtension(e.manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(nil, e.validOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if owner is invalid", func() {
		ext, err := extension.NewExtension(e.manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.invalidOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if repo is invalid", func() {
		ext, err := extension.NewExtension(e.manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.invalidRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if repo is conflict", func() {
		ext, err := extension.NewExtension(e.manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.conflictRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if alias is conflict", func() {
		ext, err := extension.NewExtension(e.manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.conflictAlias)

		e.Error(err)
	})

	e.Run("should return error if extension is already installed", func() {
		manifest := &extension.Manifest{
			Metadatas: []*extension.Metadata{
				{
					Owner: e.validOwner,
					Repo:  e.validRepo,
				},
			},
		}
		ext, err := extension.NewExtension(manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if alias is already used", func() {
		manifest := &extension.Manifest{
			Metadatas: []*extension.Metadata{
				{
					Owner:   e.validOwner,
					Repo:    e.validRepo,
					Aliases: []string{e.validAlias},
				},
			},
		}
		ext, err := extension.NewExtension(manifest, e.ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner+"v2", e.validRepo+"v2", e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if error when getting latest release", func() {
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		ghReleaseGetter.On(
			"GetLatestRelease", tMock.Anything, tMock.Anything, tMock.Anything,
		).Return(nil, nil, errors.New("test error"))
		ext, err := extension.NewExtension(e.manifest, ghReleaseGetter, e.httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if error when extracting download url", func() {
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		ghReleaseGetter.On(
			"GetLatestRelease", tMock.Anything, tMock.Anything, tMock.Anything,
		).Return(&github.RepositoryRelease{}, nil, nil)
		httpDoer := &mock.HTTPDoer{}
		httpDoer.On("Do", tMock.Anything).Return(nil, errors.New("error test"))
		ext, err := extension.NewExtension(e.manifest, ghReleaseGetter, httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if error when downloading asset", func() {
		tagName := "v0.0.1"
		releaseName := fmt.Sprintf("%s_%s-%s", tagName, runtime.GOOS, runtime.GOARCH)
		downloadURL := "http://localhost:8080"
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		ghReleaseGetter.On(
			"GetLatestRelease", tMock.Anything, tMock.Anything, tMock.Anything,
		).Return(&github.RepositoryRelease{
			Assets: []github.ReleaseAsset{
				{
					Name:               &releaseName,
					BrowserDownloadURL: &downloadURL,
				},
			},
			TagName: &tagName,
		}, nil, nil)
		httpDoer := &mock.HTTPDoer{}
		httpDoer.On("Do", tMock.Anything).Return(nil, errors.New("error test"))
		ext, err := extension.NewExtension(e.manifest, ghReleaseGetter, httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return error if error if download response is more than or equal 300", func() {
		tagName := "v0.0.1"
		releaseName := fmt.Sprintf("%s_%s-%s", tagName, runtime.GOOS, runtime.GOARCH)
		downloadURL := "http://localhost:8080"
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		ghReleaseGetter.On(
			"GetLatestRelease", tMock.Anything, tMock.Anything, tMock.Anything,
		).Return(&github.RepositoryRelease{
			Assets: []github.ReleaseAsset{
				{
					Name:               &releaseName,
					BrowserDownloadURL: &downloadURL,
				},
			},
			TagName: &tagName,
		}, nil, nil)
		httpDoer := &mock.HTTPDoer{}
		httpDoer.On(
			"Do", tMock.Anything,
		).Return(&http.Response{
			StatusCode: 404,
			Body:       &http.NoBody,
		}, nil)
		ext, err := extension.NewExtension(e.manifest, ghReleaseGetter, httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.validAlias)

		e.Error(err)
	})

	e.Run("should return nil if no error is encountered", func() {
		tagName := "v0.0.1"
		releaseName := fmt.Sprintf("%s_%s-%s", tagName, runtime.GOOS, runtime.GOARCH)
		downloadURL := "http://localhost:8080"
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		ghReleaseGetter.On(
			"GetLatestRelease", tMock.Anything, tMock.Anything, tMock.Anything,
		).Return(&github.RepositoryRelease{
			Assets: []github.ReleaseAsset{
				{
					Name:               &releaseName,
					BrowserDownloadURL: &downloadURL,
				},
			},
			TagName: &tagName,
		}, nil, nil)
		httpDoer := &mock.HTTPDoer{}
		httpDoer.On(
			"Do", tMock.Anything,
		).Return(&http.Response{
			StatusCode: 200,
			Body:       &http.NoBody,
		}, nil)
		ext, err := extension.NewExtension(e.manifest, ghReleaseGetter, httpDoer, e.dirPath, e.reservedCommands...)
		if err != nil {
			panic(err)
		}

		err = ext.Install(context.Background(), e.validOwner, e.validRepo, e.validAlias)

		e.NoError(err)
	})
}

func (e *ExtensionSuite) TestRun() {
	e.Run("should return error if no executable is found from manifest", func() {
		cmd := "unknown"
		manifest := &extension.Manifest{
			Metadatas: []*extension.Metadata{
				{
					Aliases: []string{cmd},
				},
			},
		}
		ext, err := extension.NewExtension(manifest, &mock.GithubReleaseGetter{}, &mock.HTTPDoer{}, "./out")
		if err != nil {
			panic(err)
		}

		err = ext.Run(cmd, nil)

		e.Error(err)
	})
}

func (e *ExtensionSuite) TearDownSuite() {
	if err := os.RemoveAll(e.dirPath); err != nil {
		panic(err)
	}
}

func TestExtensionSuite(t *testing.T) {
	suite.Run(t, &ExtensionSuite{})
}

func TestNewExtension(t *testing.T) {
	t.Run("should return nil and error if manifest is nil", func(t *testing.T) {
		var manifest *extension.Manifest = nil
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		httpDoer := &mock.HTTPDoer{}
		dirPath := "./out"
		reservedCommands := []string{"help"}

		ext, err := extension.NewExtension(manifest, ghReleaseGetter, httpDoer, dirPath, reservedCommands...)

		assert.Nil(t, ext)
		assert.NotNil(t, err)
	})

	t.Run("should return nil and error if github release getter is nil", func(t *testing.T) {
		manifest := &extension.Manifest{}
		var ghReleaseGetter extension.GithubReleaseGetter = nil
		httpDoer := &mock.HTTPDoer{}
		dirPath := "./out"
		reservedCommands := []string{"help"}

		ext, err := extension.NewExtension(manifest, ghReleaseGetter, httpDoer, dirPath, reservedCommands...)

		assert.Nil(t, ext)
		assert.NotNil(t, err)
	})

	t.Run("should return nil and error if httpdoer is nil", func(t *testing.T) {
		manifest := &extension.Manifest{}
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		var httpDoer extension.HTTPDoer = nil
		dirPath := "./out"
		reservedCommands := []string{"help"}

		ext, err := extension.NewExtension(manifest, ghReleaseGetter, httpDoer, dirPath, reservedCommands...)

		assert.Nil(t, ext)
		assert.NotNil(t, err)
	})

	t.Run("should return nil and error if dir path is empty", func(t *testing.T) {
		manifest := &extension.Manifest{}
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		httpDoer := &mock.HTTPDoer{}
		dirPath := ""
		reservedCommands := []string{"help"}

		ext, err := extension.NewExtension(manifest, ghReleaseGetter, httpDoer, dirPath, reservedCommands...)

		assert.Nil(t, ext)
		assert.NotNil(t, err)
	})

	t.Run("should return value and nil if no error is encountered", func(t *testing.T) {
		manifest := &extension.Manifest{}
		ghReleaseGetter := &mock.GithubReleaseGetter{}
		httpDoer := &mock.HTTPDoer{}
		dirPath := "./out"
		reservedCommands := []string{"help"}

		ext, err := extension.NewExtension(manifest, ghReleaseGetter, httpDoer, dirPath, reservedCommands...)

		assert.NotNil(t, ext)
		assert.NoError(t, err)
	})
}
