package job_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
)

type ExternalDependencyGetterFactoryTestSuite struct {
	suite.Suite
}

func (m *ExternalDependencyGetterFactoryTestSuite) TestRegister() {
	m.Run("should return error if type is empty", func() {
		externalDependencyGetter := &job.ExternalDependencyGetterFactory{}

		var _type string
		newExternalDependencyGetter := func(rm config.ResourceManager) (job.ExternalDependencyGetter, error) {
			return nil, nil
		}

		actualError := externalDependencyGetter.Register(_type, newExternalDependencyGetter)

		m.Error(actualError)
	})

	m.Run("should return error if new external dependency getter function is nil", func() {
		externalDependencyGetter := &job.ExternalDependencyGetterFactory{}

		_type := "optimus"
		var newExternalDependencyGetter job.NewExternalDependencyGetter

		actualError := externalDependencyGetter.Register(_type, newExternalDependencyGetter)

		m.Error(actualError)
	})

	m.Run("should return error if external dependency getter type is already registered", func() {
		_type := "optimus"
		newExternalDependencyGetter := func(rm config.ResourceManager) (job.ExternalDependencyGetter, error) {
			return nil, nil
		}
		externalDependencyGetter := &job.ExternalDependencyGetterFactory{}
		externalDependencyGetter.Register(_type, newExternalDependencyGetter)

		actualError := externalDependencyGetter.Register(_type, newExternalDependencyGetter)

		m.Error(actualError)
	})

	m.Run("should return nil if no error is encountered", func() {
		externalDependencyGetter := &job.ExternalDependencyGetterFactory{}

		_type := "optimus"
		newExternalDependencyGetter := func(rm config.ResourceManager) (job.ExternalDependencyGetter, error) {
			return nil, nil
		}

		actualError := externalDependencyGetter.Register(_type, newExternalDependencyGetter)

		m.NoError(actualError)
	})
}

func (m *ExternalDependencyGetterFactoryTestSuite) TestGet() {
	m.Run("should return nil and error if external dependency getter type is not registered", func() {
		externalDependencyGetter := &job.ExternalDependencyGetterFactory{}

		conf := config.ResourceManager{}

		actualResourcemanager, actualError := externalDependencyGetter.Get(conf)

		m.Nil(actualResourcemanager)
		m.Error(actualError)
	})

	m.Run("should return external dependency getter and error based on the new external dependency getter function", func() {
		conf := config.ResourceManager{
			Type: "optimus",
		}
		newExternalDependencyGetter := func(rm config.ResourceManager) (job.ExternalDependencyGetter, error) {
			return mock.NewExternalDependencyGetter(m.T()), nil
		}
		externalDependencyGetter := &job.ExternalDependencyGetterFactory{}
		externalDependencyGetter.Register(conf.Type, newExternalDependencyGetter)

		actualResourcemanager, actualError := externalDependencyGetter.Get(conf)

		m.NotNil(actualResourcemanager)
		m.NoError(actualError)
	})
}

func TestExternalDependencyGetterFactory(t *testing.T) {
	suite.Run(t, &ExternalDependencyGetterFactoryTestSuite{})
}
