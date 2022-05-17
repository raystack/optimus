package exd_test

import (
	"context"
	"errors"

	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

func (m *ManagerTestSuite) TestInstall() {
	defaultParser := exd.ParseRegistry
	defer func() { exd.ParseRegistry = defaultParser }()
	defaultNewClient := exd.NewClientRegistry
	defer func() { exd.NewClientRegistry = defaultNewClient }()

	verbose := true

	m.Run("should return error if one or more required fields are empty", func() {
		manager := &exd.Manager{}
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

		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var remotePath string
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error extracting remote metadata", func() {
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return nil, errors.New("extraction failed")
			},
		}

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if no parser could recognize remote path", func() {
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return nil, exd.ErrUnrecognizedRemotePath
			},
		}

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error loading manifest", func() {
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return &exd.Metadata{}, nil
			},
		}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		newClientFactory := &exd.NewClientFactory{}
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
			OwnerName:    "odpf",
			ProjectName:  "optimus-extension-valor",
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		commandName := "valor"
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Name:     "odpf",
					Provider: provider,
					Projects: []*exd.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*exd.RepositoryRelease{release},
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
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose, reservedCommands...)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if command name is already used by different owner project", func() {
		provider := "testing"
		metadata := &exd.Metadata{
			ProviderName: provider,
			OwnerName:    "gojek",
			ProjectName:  "optimus-extension-valor",
			TagName:      "",
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		commandName := "valor"
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Name:     "odpf",
					Provider: provider,
					Projects: []*exd.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*exd.RepositoryRelease{release},
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
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if remote path is already installed", func() {
		provider := "testing"
		metadata := &exd.Metadata{
			ProviderName: provider,
			OwnerName:    "gojek",
			ProjectName:  "optimus-extension-valor",
			TagName:      "",
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		client := &mock.Client{}
		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		commandName := "valor"
		manifest := &exd.Manifest{
			RepositoryOwners: []*exd.RepositoryOwner{
				{
					Name:     "gojek",
					Provider: provider,
					Projects: []*exd.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*exd.RepositoryRelease{release},
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
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return error if error when downloading asset", func() {
		provider := "testing"
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return(nil, errors.New("random error"))
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		assetOperator := &mock.AssetOperator{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
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
		metadata := &exd.Metadata{
			ProviderName: provider,
		}
		exd.ParseRegistry = []exd.Parser{
			func(remotePath string) (*exd.Metadata, error) {
				return metadata, nil
			},
		}

		release := &exd.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything).Return([]byte{}, nil)
		newClientFactory := &exd.NewClientFactory{}
		newClientFactory.Add(provider, func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return client, nil
		})
		exd.NewClientRegistry = newClientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&exd.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manager, err := exd.NewManager(ctx, httpDoer, manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(remotePath, commandName)

		m.NoError(actualErr)
	})
}
