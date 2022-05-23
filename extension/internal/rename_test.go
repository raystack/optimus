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

type RenameManagerTestSuite struct {
	suite.Suite
}

func (r *RenameManagerTestSuite) TestRename() {
	verbose := true

	r.Run("should return error if source command is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		var source string
		target := "valor2"

		actualErr := manager.Rename(source, target)

		r.Error(actualErr)
	})

	r.Run("should return error if target command is empty", func() {
		manifester := &mock.Manifester{}

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		var target string

		actualErr := manager.Rename(source, target)

		r.Error(actualErr)
	})

	r.Run("should return error if target command is already reserved", func() {
		manifester := &mock.Manifester{}

		reservedCommands := []string{"valor2"}
		manager, err := internal.NewRenameManager(manifester, verbose, reservedCommands...)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		r.Error(actualErr)
	})

	r.Run("should return nil if source and target is the same", func() {
		manifester := &mock.Manifester{}

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor"

		actualErr := manager.Rename(source, target)

		r.NoError(actualErr)
	})

	r.Run("should return error if error when loading manifest", func() {
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(nil, errors.New("random error"))

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		r.Error(actualErr)
	})

	r.Run("should return error if target command is alread used by other project", func() {
		project1 := &model.RepositoryProject{
			Name:        "extension1",
			CommandName: "valor",
		}
		project2 := &model.RepositoryProject{
			Name:        "extension2",
			CommandName: "valor2",
		}
		owner := &model.RepositoryOwner{
			Name:     "owner",
			Projects: []*model.RepositoryProject{project1, project2},
		}
		project1.Owner = owner
		project2.Owner = owner
		manifest := &model.Manifest{
			RepositoryOwners: []*model.RepositoryOwner{owner},
		}
		manifester := &mock.Manifester{}
		manifester.On("Load", tMock.Anything).Return(manifest, nil)

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		r.Error(actualErr)
	})

	r.Run("should return error if error encountered during updating manifest", func() {
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
		manifester.On("Flush", tMock.Anything, tMock.Anything).Return(errors.New("random error"))

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		r.Error(actualErr)
	})

	r.Run("should return error if error encountered during updating manifest", func() {
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

		manager, err := internal.NewRenameManager(manifester, verbose)
		if err != nil {
			panic(err)
		}

		source := "valor"
		target := "valor2"

		actualErr := manager.Rename(source, target)

		r.NoError(actualErr)
	})
}

func TestNewRenameManager(t *testing.T) {
	verbose := true

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		var manifester model.Manifester

		actualManager, actualErr := internal.NewRenameManager(manifester, verbose)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		manifester := &mock.Manifester{}

		actualManager, actualErr := internal.NewRenameManager(manifester, verbose)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestRenameManager(t *testing.T) {
	suite.Run(t, &RenameManagerTestSuite{})
}
