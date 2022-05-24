package extension_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/extension"
	"github.com/odpf/optimus/extension/factory"
	"github.com/odpf/optimus/extension/model"
	"github.com/odpf/optimus/mock"
)

type ManagerTestSuite struct {
	suite.Suite
}

func (m *ManagerTestSuite) TestInstall() {
	defaultParser := factory.ParseRegistry
	defer func() { factory.ParseRegistry = defaultParser }()
	defaultClient := factory.ClientRegistry
	defer func() { factory.ClientRegistry = defaultClient }()

	verbose := false

	m.Run("should return error if encountered error during execution", func() {
		ctx := context.Background()
		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"
		manager := &extension.Manager{}

		actualErr := manager.Install(ctx, remotePath, commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error is encountered", func() {
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
		manager, err := extension.NewManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		remotePath := "gojek/optimus-extension-valor"
		commandName := "valor"

		actualErr := manager.Install(ctx, remotePath, commandName)

		m.NoError(actualErr)
	})
}

func (m *ManagerTestSuite) TestUpgrade() {
	defaultParser := factory.ParseRegistry
	defer func() { factory.ParseRegistry = defaultParser }()
	defaultNewClient := factory.ClientRegistry
	defer func() { factory.ClientRegistry = defaultNewClient }()

	ctx := context.Background()
	verbose := false

	m.Run("should return error if encountered error during execution", func() {
		commandName := "valor"
		manager := &extension.Manager{}

		actualErr := manager.Upgrade(ctx, commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered", func() {
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

		manager, err := extension.NewManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Upgrade(ctx, commandName)

		m.NoError(actualErr)
	})
}

func (m *ManagerTestSuite) TestUninstall() {
	verbose := false

	m.Run("should return error if encountered error during execution", func() {
		commandName := "valor"
		tagName := "v1.0"
		manager := &extension.Manager{}

		actualErr := manager.Uninstall(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered during the whole process", func() {

		release := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		project := &model.RepositoryProject{
			CommandName:   "valor",
			ActiveTagName: "v1.0",
			Releases:      []*model.RepositoryRelease{release},
		}
		release.Project = project
		owner := &model.RepositoryOwner{
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Uninstall", tMock.Anything).Return(nil)

		manager, err := extension.NewManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		m.NoError(actualErr)
	})
}

func (m *ManagerTestSuite) TestRun() {
	verbose := false

	m.Run("should return error if encountered error during execution", func() {
		commandName := "valor"
		manager := &extension.Manager{}

		actualErr := manager.Run(commandName)

		m.Error(actualErr)
	})

	m.Run("should return nil if no error encountered during run", func() {
		project := &model.RepositoryProject{
			CommandName: "valor",
		}
		owner := &model.RepositoryOwner{
			Name:     "gojek",
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Run", tMock.Anything, tMock.Anything).Return(nil)

		manager, err := extension.NewManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"

		actualErr := manager.Run(commandName)

		m.NoError(actualErr)
	})
}

func (m *ManagerTestSuite) TestRename() {
	assetOperator := &mock.AssetOperator{}
	verbose := false

	m.Run("should return error if encountered error during execution", func() {
		source := "valor"
		target := "valor2"
		manager := &extension.Manager{}

		actualErr := manager.Rename(source, target)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		project := &model.RepositoryProject{
			Name:        "extension1",
			CommandName: "valor",
		}
		owner := &model.RepositoryOwner{
			Name:     "owner",
			Projects: []*model.RepositoryProject{project},
		}
		project.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(nil)

		manager, err := extension.NewManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		m.NoError(actualErr)
	})
}

func (m *ManagerTestSuite) TestActivate() {
	assetOperator := &mock.AssetOperator{}
	verbose := false

	m.Run("should return error if encountered error during execution", func() {
		commandName := "valor"
		tagName := "v1"
		manager := &extension.Manager{}

		actualErr := manager.Activate(commandName, tagName)

		m.Error(actualErr)
	})

	m.Run("should return error if error encountered during updating manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(&model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{
				{
					Projects: []*model.RepositoryProject{
						{
							ActiveTagName: "v2",
							CommandName:   "valor",
							Releases: []*model.RepositoryRelease{
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

		manager, err := extension.NewManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1"

		actualErr := manager.Activate(commandName, tagName)

		m.NoError(actualErr)
	})
}

func TestNewManager(t *testing.T) {
	verbose := false

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := extension.NewManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if asset operator is nil", func(t *testing.T) {
		manifester := &mock.Manifester{}
		var assetOperator model.AssetOperator

		actualManager, actualErr := extension.NewManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := extension.NewManager(manifester, assetOperator, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestManager(t *testing.T) {
	suite.Run(t, &ManagerTestSuite{})
}
