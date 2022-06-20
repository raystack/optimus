package internal_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/extension/internal"
	"github.com/odpf/optimus/extension/model"
	"github.com/odpf/optimus/mock"
)

type UninstallManagerTestSuite struct {
	suite.Suite
}

func (u *UninstallManagerTestSuite) TestUninstall() {
	verbose := true

	u.Run("should return error if command name is empty", func() {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		var commandName string
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return error if error loading manifest", func() {
		assetOperator := &mock.AssetOperator{}

		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return error if command name is not found", func() {
		assetOperator := &mock.AssetOperator{}

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

		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor2"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return error if tag name is not found", func() {
		assetOperator := &mock.AssetOperator{}

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

		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.1"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return error if error encountered during preparation", func() {
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

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(errors.New("random error"))

		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return error if error encountered during uninstallation", func() {
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

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Uninstall", tMock.Anything).Return(errors.New("random error"))

		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return error if error encountered during updating manifest", func() {
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
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		assetOperator := &mock.AssetOperator{}
		assetOperator.On("Prepare", tMock.Anything).Return(nil)
		assetOperator.On("Uninstall", tMock.Anything).Return(nil)

		verbose := false
		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.Error(actualErr)
	})

	u.Run("should return nil if no error encountered during the whole process", func() {
		release1 := &model.RepositoryRelease{
			TagName: "v1.0",
		}
		release2 := &model.RepositoryRelease{
			TagName: "v1.1",
		}
		project := &model.RepositoryProject{
			CommandName:   "valor",
			ActiveTagName: "v1.0",
			Releases:      []*model.RepositoryRelease{release1, release2},
		}
		release1.Project = project
		release2.Project = project
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

		verbose := false
		manager, err := internal.NewUninstallManager(manifester, assetOperator, verbose)
		if err != nil {
			panic(err)
		}

		commandName := "valor"
		tagName := "v1.0"

		actualErr := manager.Uninstall(commandName, tagName)

		u.NoError(actualErr)
	})
}

func TestNewUninstallManager(t *testing.T) {
	verbose := true

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewUninstallManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if asset operator is nil", func(t *testing.T) {
		manifester := &mock.Manifester{}
		var assetOperator model.AssetOperator

		actualManager, actualErr := internal.NewUninstallManager(manifester, assetOperator, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}
		assetOperator := &mock.AssetOperator{}

		actualManager, actualErr := internal.NewUninstallManager(manifester, assetOperator, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestUninstallManager(t *testing.T) {
	suite.Run(t, &UninstallManagerTestSuite{})
}
