package internal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/client/extension/factory"
	"github.com/odpf/optimus/client/extension/internal"
	"github.com/odpf/optimus/client/extension/model"
	"github.com/odpf/optimus/mock"
)

type InstallManagerTestSuite struct {
	suite.Suite
}

func (i *InstallManagerTestSuite) TestInstall() {
	defaultParser := factory.ParseRegistry
	defer func() { factory.ParseRegistry = defaultParser }()
	defaultClient := factory.ClientRegistry
	defer func() { factory.ClientRegistry = defaultClient }()

	verbose := true

	i.Run("should return error if remote path is empty", func() {
		ctx := context.Background()
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var remotePath string
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error encountered during extracting remote metadata", func() {
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return nil, errors.New("extraction failed")
			},
		}

		ctx := context.Background()
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if no parser could recognize remote path", func() {
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return nil, model.ErrUnrecognizedRemotePath
			},
		}

		ctx := context.Background()
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error loading manifest", func() {
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return &model.Metadata{}, nil
			},
		}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error getting client", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		factory.ClientRegistry = &factory.ClientFactory{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error downloading release", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(nil, errors.New("random error"))
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if command name part of reserved command", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
			OwnerName:    "odpf",
			ProjectName:  "optimus-extension-valor",
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		commandName := "valor"
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{
				{
					Name:     "odpf",
					Provider: provider,
					Projects: []*model.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*model.RepositoryRelease{release},
						},
					},
				},
			},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		reservedCommands := []string{"valor"}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose, reservedCommands...)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if command name is already used by different owner project", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
			OwnerName:    "gojek",
			ProjectName:  "optimus-extension-valor",
			TagName:      "",
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		commandName := "valor"
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{
				{
					Name:     "odpf",
					Provider: provider,
					Projects: []*model.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*model.RepositoryRelease{release},
						},
					},
				},
			},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if remote path is already installed", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
			OwnerName:    "gojek",
			ProjectName:  "optimus-extension-valor",
			TagName:      "",
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		client := &mock.Client{}
		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		commandName := "valor"
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{
				{
					Name:     "gojek",
					Provider: provider,
					Projects: []*model.RepositoryProject{
						{
							Name:          "optimus-extension-valor",
							CommandName:   commandName,
							ActiveTagName: "v1.0",
							Releases:      []*model.RepositoryRelease{release},
						},
					},
				},
			},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error when downloading asset", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return(nil, errors.New("random error"))
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		ctx := context.Background()
		assetOperator := &mock.AssetOperator{}
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error when preparing installation", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error when executing installation", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		ctx := context.Background()
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should return error if error encountered during updating manifest", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		verbose := false
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.Error(actualErr)
	})

	i.Run("should update manifest and return nil if no error is encountered", func() {
		provider := "testing"
		metadata := &model.Metadata{
			ProviderName: provider,
		}
		factory.ParseRegistry = []model.Parser{
			func(remotePath string) (*model.Metadata, error) {
				return metadata, nil
			},
		}

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		ctx := context.Background()
		verbose := false
		manager, err := internal.NewInstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		i.NoError(actualErr)
	})
}

func TestNewInstallManager(t *testing.T) {
	verbose := true

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewInstallManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if asset operator is nil", func(t *testing.T) {
		manifester := &mock.Manifester{}
		var assetOperator model.AssetOperator

		actualManager, actualErr := internal.NewInstallManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewInstallManager(manifester, assetOperator, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestInstallManager(t *testing.T) {
	suite.Run(t, &InstallManagerTestSuite{})
}
