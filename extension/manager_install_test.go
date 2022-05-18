package extension_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestInstall() {
	defaultParser := extension.ParseRegistry
	defer func() { extension.ParseRegistry = defaultParser }()
	defaultNewClient := extension.NewClientRegistry
	defer func() { extension.NewClientRegistry = defaultNewClient }()

	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &extension.Manager{}
		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if remote path is empty", func() {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var remotePath string
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error extracting remote metadata", func() {
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return nil, errors.New("extraction failed")
			},
		}

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if no parser could recognize remote path", func() {
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return nil, extension.ErrUnrecognizedRemotePath
			},
		}

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error loading manifest", func() {
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return &extension.Metadata{}, nil
			},
		}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error getting new client", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		newClientFactory := &extension.NewClientFactory{}
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error downloading release", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name part of reserved command", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
			OwnerName:    "odpf",
			ProjectName:  "optimus-extension-valor",
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		commandName := "valor"
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{
				{
					Name:     "odpf",
					Provider: provider,
					Projects: []*extension.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*extension.RepositoryRelease{release},
						},
					},
				},
			},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		reservedCommands := []string{"valor"}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose, reservedCommands...)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is already used by different owner project", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
			OwnerName:    "gojek",
			ProjectName:  "optimus-extension-valor",
			TagName:      "",
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		commandName := "valor"
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{
				{
					Name:     "odpf",
					Provider: provider,
					Projects: []*extension.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*extension.RepositoryRelease{release},
						},
					},
				},
			},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if remote path is already installed", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
			OwnerName:    "gojek",
			ProjectName:  "optimus-extension-valor",
			TagName:      "",
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		client := &mock.Client{}
		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		commandName := "valor"
		manifest := &extension.Manifest{
			RepositoryOwners: []*extension.RepositoryOwner{
				{
					Name:     "gojek",
					Provider: provider,
					Projects: []*extension.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*extension.RepositoryRelease{release},
						},
					},
				},
			},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when downloading asset", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when preparing installation", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when executing installation", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should update manifest and return nil if no error is encountered", func() {
		provider := "testing"
		metadata := &extension.Metadata{
			ProviderName: provider,
		}
		extension.ParseRegistry = []extension.Parser{
			func(remotePath string) (*extension.Metadata, error) {
				return metadata, nil
			},
		}

		release := &extension.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &extension.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return client, nil
		})
		extension.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&extension.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := extension.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.NoError(actualErr)
	})
}
