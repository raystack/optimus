package internal_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/extension/internal"
	"github.com/odpf/optimus/extension/model"
	"github.com/odpf/optimus/mock"
)

type ActivateManagerTestSuite struct {
	suite.Suite
}

func (r *ActivateManagerTestSuite) TestActivate() {
	verbose := true

	r.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		r.Error(actualErr)
	})

	r.Run("should return error if tag name is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		var tagName string

		actualErr := manager.Activate(commandName, tagName)

		r.Error(actualErr)
	})

	r.Run("should return error if error loading manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		r.Error(actualErr)
	})

	r.Run("should return error if command name is not found", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		r.Error(actualErr)
	})

	r.Run("should return error if tag name is not installed", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		r.Error(actualErr)
	})

	r.Run("should return error if error encountered during updating manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{
				{
					Projects: []*model.RepositoryProject{
						{
							ActiveTagName: "v2",
							CommandName:   "valor",
							Releases: []*model.RepositoryRelease{
								{
									TagName: "v2",
								},
								{
									TagName: "v1",
								},
							},
						},
					},
				},
			},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		verbose := false
		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		r.Error(actualErr)
	})

	r.Run("should return nil if no error is encountered", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{
				{
					Projects: []*model.RepositoryProject{
						{
							ActiveTagName: "v2",
							CommandName:   "valor",
							Releases: []*model.RepositoryRelease{
								{
									TagName: "v2",
								},
								{
									TagName: "v1",
								},
							},
						},
					},
				},
			},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		verbose := false
		manager, err := internal.NewActivateManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		r.NoError(actualErr)
	})
}

func TestNewActivateManager(t *testing.T) {
	verbose := true

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester

		actualManager, actualErr := internal.NewActivateManager(manifester, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}

		actualManager, actualErr := internal.NewActivateManager(manifester, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestActivateManager(t *testing.T) {
	suite.Run(t, &ActivateManagerTestSuite{})
}
