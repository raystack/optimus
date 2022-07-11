package resourcemgr_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/mock"
)

type ManagerFactoryTestSuite struct {
	suite.Suite
}

func (m *ManagerFactoryTestSuite) TestRegister() {
	m.Run("should return error if type is empty", func() {
		manager := &resourcemgr.ManagerFactory{}

		var _type string
		newResourceManager := func(conf interface{}) (resourcemgr.ResourceManager, error) {
			return nil, nil
		}

		actualError := manager.Register(_type, newResourceManager)

		m.Error(actualError)
	})

	m.Run("should return error if resource manager is nil", func() {
		manager := &resourcemgr.ManagerFactory{}

		_type := "optimus"
		var newResourceManager resourcemgr.NewResourceManager

		actualError := manager.Register(_type, newResourceManager)

		m.Error(actualError)
	})

	m.Run("should return error if resource manager type is already registered", func() {
		manager := &resourcemgr.ManagerFactory{}

		_type := "optimus"
		newResourceManager := func(conf interface{}) (resourcemgr.ResourceManager, error) {
			return nil, nil
		}
		manager.Register(_type, newResourceManager)

		actualError := manager.Register(_type, newResourceManager)

		m.Error(actualError)
	})

	m.Run("should return nil if no error is encountered", func() {
		manager := &resourcemgr.ManagerFactory{}

		_type := "optimus"
		newResourceManager := func(conf interface{}) (resourcemgr.ResourceManager, error) {
			return nil, nil
		}

		actualError := manager.Register(_type, newResourceManager)

		m.NoError(actualError)
	})
}

func (m *ManagerFactoryTestSuite) TestGet() {
	m.Run("should return nil and error if resource manager type is not registered", func() {
		manager := &resourcemgr.ManagerFactory{}

		_type := "optimus"
		conf := config.ResourceManagerConfigOptimus{}

		actualResourcemanager, actualError := manager.Get(_type, conf)

		m.Nil(actualResourcemanager)
		m.Error(actualError)
	})

	m.Run("should return resource manager and error based on the new resource manager function", func() {
		manager := &resourcemgr.ManagerFactory{}

		_type := "optimus"
		conf := config.ResourceManagerConfigOptimus{}
		manager.Register(_type, func(conf interface{}) (resourcemgr.ResourceManager, error) {
			return mock.NewResourceManager(m.T()), nil
		})

		actualResourcemanager, actualError := manager.Get(_type, conf)

		m.NotNil(actualResourcemanager)
		m.NoError(actualError)
	})
}

func TestManagerFactory(t *testing.T) {
	suite.Run(t, &ManagerFactoryTestSuite{})
}
