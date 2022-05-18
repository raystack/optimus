package extension_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestRename() {
	ctx := context.Background()
	httpDoer := &mock.HTTPDoer{}
	assetOperator := &mock.AssetOperator{}
	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &extension.Manager{}
		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if source command is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var source string
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if target command is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		var target string

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if target command is already reserved", func() {
		manifester := &mock.Manifester{}

		reservedCommands := []string{"valor2"}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose, reservedCommands...)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return nil if source and target is the same", func() {
		manifester := &mock.Manifester{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor"

		actualErr := manager.Rename(source, target)

		m.NoError(actualErr)
	})

	m.Run("should return error if error when loading manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if target command is alread used by other project", func() {
		project1 := &extension.RepositoryProject{
			Name:        "extension1",
			CommandName: "valor",
		}
		project2 := &extension.RepositoryProject{
			Name:        "extension2",
			CommandName: "valor2",
		}
		owner := &extension.RepositoryOwner{
			Name:     "owner",
			Projects: []*extension.RepositoryProject{project1, project2},
		}
		project1.Owner = owner
		project2.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		project := &extension.RepositoryProject{
			Name:        "extension1",
			CommandName: "valor",
		}
		owner := &extension.RepositoryOwner{
			Name:     "owner",
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		project := &extension.RepositoryProject{
			Name:        "extension1",
			CommandName: "valor",
		}
		owner := &extension.RepositoryOwner{
			Name:     "owner",
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.NoError(actualErr)
	})
}
