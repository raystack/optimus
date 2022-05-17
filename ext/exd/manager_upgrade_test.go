package exd_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestUpgrade() {
	defaultParser := exd.ParseRegistry
	defer func() { exd.ParseRegistry = defaultParser }()
	defaultNewClient := exd.NewClientRegistry
	defer func() { exd.NewClientRegistry = defaultNewClient }()

	ctx := context.Background()
	httpDoer := &mock.HTTPDoer{}
	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &exd.Manager{}
		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error loading manifest", func() {
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is not found", func() {
		installer := &mock.Installer{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error getting new client", func() {
		provider := "github"

		project := &exd.RepositoryProject{
			Name:        "optimus-extension-valor",
			CommandName: "valor",
		}
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		newClientFactory := &exd.NewClientFactory{}
		exd.NewClientRegistry = newClientFactory

		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if no current release is found within the active tag", func() {
		provider := "github"

		release := &exd.RepositoryRelease{
			TagName: "v0.0.1",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return nil, nil
		})
		exd.NewClientRegistry = newClientFactory

		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when downloading upgrade release", func() {
		provider := "github"

		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if the latest upgrade is already installed but error when updating manifest", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if the latest upgrade is already installed and no error when updating manifest", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.NoError(actualErr)
	})

	m.Run("should return error if encountered error when downloading asset", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&exd.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		installer := &mock.Installer{}

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if encountered error preparing installation", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&exd.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}, nil)

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if encountered error when installing upgrade asset", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&exd.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}, nil)

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(nil)
		installer.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when updating manifest on success installation", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&exd.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(nil)
		installer.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered during the whole upgrade process", func() {
		provider := "github"
		release := &exd.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &exd.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*exd.RepositoryRelease{release},
		}
		release.Project = project
		owner := &exd.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*exd.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&exd.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return(nil, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{owner},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		installer := &mock.Installer{}
		installer.On("Prepare", tMock.Anything).Return(nil)
		installer.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		manager, err := exd.NewManager(ctx, httpDoer, manifester, installer, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.NoError(actualErr)
	})
}
