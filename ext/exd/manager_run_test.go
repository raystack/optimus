package exd_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestRun() {
	ctx := context.Background()
	httpDoer := &mock.HTTPDoer{}
	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &exd.Manager{}
		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during preparation", func() {
		project := &exd.RepositoryProject{
			CommandName: "valor",
		}
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during run", func() {
		project := &exd.RepositoryProject{
			CommandName: "valor",
		}
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered during run", func() {
		project := &exd.RepositoryProject{
			CommandName: "valor",
		}
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.NoError(actualErr)
	})
}
