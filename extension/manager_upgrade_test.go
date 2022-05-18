package extension_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestUpgrade() {
	defaultParser := extension.ParseRegistry
	defer func() { extension.ParseRegistry = defaultParser }()
	defaultNewClient := extension.NewClientRegistry
	defer func() { extension.NewClientRegistry = defaultNewClient }()

	ctx := context.Background()
	httpDoer := &mock.HTTPDoer{}
	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &extension.Manager{}
		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

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

		actualErr := manager.Upgrade(commandName)

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

		actualErr := manager.Upgrade(commandName)

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

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error getting new client", func() {
		provider := "github"

		project := &extension.RepositoryProject{
			Name:        "optimus-extension-valor",
			CommandName: "valor",
		}
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		newClientFactory := &extension.NewClientFactory{}
		extension.NewClientRegistry = newClientFactory

		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if no current release is found within the active tag", func() {
		provider := "github"

		release := &extension.RepositoryRelease{
			TagName: "v0.0.1",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return nil, nil
		})
		extension.NewClientRegistry = newClientFactory

		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when downloading upgrade release", func() {
		provider := "github"

		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if the latest upgrade is already installed but error when updating manifest", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if the latest upgrade is already installed and no error when updating manifest", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.NoError(actualErr)
	})

	m.Run("should return error if encountered error when downloading asset", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&extension.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if encountered error preparing installation", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&extension.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if encountered error when installing upgrade asset", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&extension.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when updating manifest on success installation", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&extension.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered during the whole upgrade process", func() {
		provider := "github"
		release := &extension.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &extension.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*extension.RepositoryRelease{release},
		}
		release.Project = project
		owner := &extension.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*extension.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(&extension.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything).Return(nil, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{owner},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(commandName)

		m.NoError(actualErr)
	})
}
