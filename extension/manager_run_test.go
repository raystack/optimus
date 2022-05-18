package extension_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestRun() {
	ctx := context.Background()
	httpDoer := &mock.HTTPDoer{}
	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &extension.Manager{}
		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error loading manifest", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is not found", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during preparation", func() {
		project := &extension.RepositoryProject{
			CommandName: "valor",
		}
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during run", func() {
		project := &extension.RepositoryProject{
			CommandName: "valor",
		}
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered during run", func() {
		project := &extension.RepositoryProject{
			CommandName: "valor",
		}
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(nil)

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.NoError(actualErr)
	})
}
