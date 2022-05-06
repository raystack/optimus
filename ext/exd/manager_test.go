package exd_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/exd"
	"github.com/odpf/optimus/mock"
)

type ManagerTestSuite struct {
	suite.Suite
}

func TestNewManager(t *testing.T) {
	t.Run("should return nil and error if context is nil", func(t *testing.T) {
		var ctx context.Context
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if http doer is nil", func(t *testing.T) {
		ctx := context.Background()
		var httpDoer exd.HTTPDoer
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if manifester is nil", func(t *testing.T) {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		var manifester exd.Manifester
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return nil and error if installer is nil", func(t *testing.T) {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		var installer exd.Installer

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.Nil(t, actualManager)
		assert.Error(t, actualErr)
	})

	t.Run("should return manager and nil if no error encountered", func(t *testing.T) {
		ctx := context.Background()
		httpDoer := &mock.HTTPDoer{}
		manifester := &mock.Manifester{}
		installer := &mock.Installer{}

		actualManager, actualErr := exd.NewManager(ctx, httpDoer, manifester, installer)

		assert.NotNil(t, actualManager)
		assert.NoError(t, actualErr)
	})
}

func TestManager(t *testing.T) {
	suite.Run(t, &ManagerTestSuite{})
}
