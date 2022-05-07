package exd_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestRename() {
	m.Run("should return error if one or more required fields are empty", func() {
		manager := &exd.Manager{}
		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if source command is empty", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		var source string
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if target command is empty", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		source := "valor"
		var target string

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return nil if source and target is the same", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor"

		actualErr := manager.Rename(source, target)

		m.NoError(actualErr)
	})

	m.Run("should return error if error when loading manifest", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if target command is alread used by other project", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Name: "project1",
					Projects: []*exd.RepositoryProject{
						{
							Name:        "extension1",
							CommandName: "valor",
						},
						{
							Name:        "extension2",
							CommandName: "valor2",
						},
					},
				},
			},
		}, nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Name: "project1",
					Projects: []*exd.RepositoryProject{
						{
							Name:        "extension1",
							CommandName: "valor",
						},
					},
				},
			},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Name: "project1",
					Projects: []*exd.RepositoryProject{
						{
							Name:        "extension1",
							CommandName: "valor",
						},
					},
				},
			},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.NoError(actualErr)
	})
}
