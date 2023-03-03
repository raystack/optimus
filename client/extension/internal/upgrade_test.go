package internal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/goto/optimus/client/extension/factory"
	"github.com/goto/optimus/client/extension/internal"
	"github.com/goto/optimus/client/extension/mock"
	"github.com/goto/optimus/client/extension/model"
)

type UpgradeManagerTestSuite struct {
	suite.Suite
}

func (u *UpgradeManagerTestSuite) TestUpgrade() {
	defaultParser := factory.ParseRegistry
	defer func() { factory.ParseRegistry = defaultParser }()
	defaultClient := factory.ClientRegistry
	defer func() { factory.ClientRegistry = defaultClient }()

	ctx := context.Background()
	verbose := true

	u.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if error loading manifest", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if command name is not found", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{}, nil)

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if error getting new client", func() {
		provider := "github"

		project := &model.RepositoryProject{
			Name:        "optimus-extension-valor",
			CommandName: "valor",
		}
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		clientFactory := &factory.ClientFactory{}
		factory.ClientRegistry = clientFactory

		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if no current release is found within the active tag", func() {
		provider := "github"

		release := &model.RepositoryRelease{
			TagName: "v0.0.1",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		client := &mock.Client{}
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if error when downloading upgrade release", func() {
		provider := "github"

		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(nil, errors.New("random error"))
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if the latest upgrade is already installed but error when updating manifest", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project1 := &model.RepositoryProject{
			Name:          "optimus-extension-valor1",
			CommandName:   "valor1",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		project2 := &model.RepositoryProject{
			Name:          "optimus-extension-valor2",
			CommandName:   "valor2",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		project3 := &model.RepositoryProject{
			Name:          "optimus-extension-valor3",
			CommandName:   "valor3",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project3
		owner1 := &model.RepositoryOwner{
			Name:     "goto",
			Provider: provider,
			Projects: []*model.RepositoryProject{project1},
		}
		project1.Owner = owner1
		owner2 := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project2, project3},
		}
		project2.Owner = owner2
		project3.Owner = owner2
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner1, owner2},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor3"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return nil if the latest upgrade is already installed and no error when updating manifest", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(release, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.NoError(actualErr)
	})

	u.Run("should return error if encountered error when downloading asset", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(&model.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return(nil, errors.New("random error"))
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if encountered error preparing installation", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(&model.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if encountered error when installing upgrade asset", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(&model.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return error if error when updating manifest on success installation", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(&model.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return([]byte{}, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		verbose := false
		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.Error(actualErr)
	})

	u.Run("should return nil if no error encountered during the whole upgrade process", func() {
		provider := "github"
		release := &model.RepositoryRelease{
			TagName: "v1.0.0",
		}
		project := &model.RepositoryProject{
			Name:          "optimus-extension-valor",
			CommandName:   "valor",
			ActiveTagName: "v1.0.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Provider: provider,
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner

		client := &mock.Client{}
		client.On("DownloadRelease", tMock.Anything, tMock.Anything).Return(&model.RepositoryRelease{
			TagName: "v1.0.1",
		}, nil)
		client.On("DownloadAsset", tMock.Anything, tMock.Anything).Return(nil, nil)
		clientFactory := &factory.ClientFactory{}
		clientFactory.Add(provider, client)
		factory.ClientRegistry = clientFactory

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Install", tMock.Anything, tMock.Anything, tMock.Anything).Return(nil)

		verbose := false
		manager, err := internal.NewUpgradeManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		u.NoError(actualErr)
	})
}

func TestNewUpgradeManager(t *testing.T) {
	verbose := true

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewUpgradeManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if asset operator is nil", func(t *testing.T) {
		manifester := &mock.Manifester{}
		var assetOperator model.AssetOperator

		actualManager, actualErr := internal.NewUpgradeManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewUpgradeManager(manifester, assetOperator, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestUpgradeManager(t *testing.T) {
	suite.Run(t, &UpgradeManagerTestSuite{})
}
