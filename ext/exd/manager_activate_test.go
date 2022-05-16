package exd_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestActivate() {
	ctx := context.Background()
	httpDoer := &mock.HTTPDoer{}
	installer := &mock.Installer{}
	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &exd.Manager{}
		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if tag name is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		var tagName string

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if error loading manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is not found", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if tag name is not installed", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Projects: []*exd.RepositoryProject{
						{
							ActiveTagName: "v2",
							CommandName:   "valor",
							Releases: []*exd.RepositoryRelease{
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

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error is encountered", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Projects: []*exd.RepositoryProject{
						{
							ActiveTagName: "v2",
							CommandName:   "valor",
							Releases: []*exd.RepositoryRelease{
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

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.NoError(actualErr)
	})
}
