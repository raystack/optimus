package internal_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/client/extension/internal"
	"github.com/odpf/optimus/client/extension/mock"
	"github.com/odpf/optimus/client/extension/model"
)

type RunManagerTestSuite struct {
	suite.Suite
}

func (r *RunManagerTestSuite) TestRun() {
	verbose := true

	r.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewRunManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string

		actualErr := manager.Run(commandName)

		r.Error(actualErr)
	})

	r.Run("should return error if error loading manifest", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := internal.NewRunManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		r.Error(actualErr)
	})

	r.Run("should return error if command name is not found", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		manager, err := internal.NewRunManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		r.Error(actualErr)
	})

	r.Run("should return error if error encountered during preparation", func() {
		project := &model.RepositoryProject{
			CommandName: "valor",
		}
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := internal.NewRunManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		r.Error(actualErr)
	})

	r.Run("should return error if error encountered during run", func() {
		project := &model.RepositoryProject{
			CommandName: "valor",
		}
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		verbose := false
		manager, err := internal.NewRunManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		r.Error(actualErr)
	})

	r.Run("should return nil if no error encountered during run", func() {
		project := &model.RepositoryProject{
			CommandName: "valor",
		}
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(nil)

		verbose := false
		manager, err := internal.NewRunManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		r.NoError(actualErr)
	})
}

func TestNewRunManager(t *testing.T) {
	verbose := true

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewRunManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if asset operator is nil", func(t *testing.T) {
		manifester := &mock.Manifester{}
		var assetOperator model.AssetOperator

		actualManager, actualErr := internal.NewRunManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewRunManager(manifester, assetOperator, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestRunManager(t *testing.T) {
	suite.Run(t, &RunManagerTestSuite{})
}
