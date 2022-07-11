package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type OptimusDependencyResolverTestSuite struct {
	suite.Suite
}

func (o *OptimusDependencyResolverTestSuite) TestFetchOptimusDependencies() {
	o.Run("should return nil and error if context is nil", func() {
		resolver, _, tearDown := o.getSetup()
		defer tearDown()

		var ctx context.Context
		filter := models.JobSpecFilter{}

		actualDependencies, actualError := resolver.FetchOptimusDependencies(ctx, filter)

		o.Nil(actualDependencies)
		o.Error(actualError)
	})

	o.Run("should return nil and error if error when getting job specifications from resource manager", func() {
		resolver, manager, tearDown := o.getSetup()
		defer tearDown()

		ctx := context.Background()
		filter := models.JobSpecFilter{}

		manager.On("GetJobSpecifications", ctx, filter).Return(nil, errors.New("random error"))

		actualDependencies, actualError := resolver.FetchOptimusDependencies(ctx, filter)

		o.Nil(actualDependencies)
		o.Error(actualError)
	})

	o.Run("should return optimus dependencies and nil if no error is encountered", func() {
		resolver, manager, tearDown := o.getSetup()
		defer tearDown()

		ctx := context.Background()
		filter := models.JobSpecFilter{}

		jobSpecs := []models.JobSpec{
			{
				Name: "job",
				NamespaceSpec: models.NamespaceSpec{
					Name: "namespace",
					ProjectSpec: models.ProjectSpec{
						Name: "project",
					},
				},
			},
		}
		manager.On("GetJobSpecifications", ctx, filter).Return(jobSpecs, nil)

		expectedDependencies := []models.OptimusDependency{
			{
				Name:          "test",
				Host:          "localhost",
				Headers:       map[string]string{"key": "value"},
				ProjectName:   "project",
				NamespaceName: "namespace",
				JobName:       "job",
			},
		}

		actualDependencies, actualError := resolver.FetchOptimusDependencies(ctx, filter)

		o.EqualValues(expectedDependencies, actualDependencies)
		o.NoError(actualError)
	})
}

func (o *OptimusDependencyResolverTestSuite) getSetup() (resolver job.OptimusDependencyResolver, manager *mock.ResourceManager, tearDown func()) {
	originalRegistry := resourcemgr.Registry
	tearDown = func() {
		resourcemgr.Registry = originalRegistry
	}

	manager = mock.NewResourceManager(o.T())

	resourcemgr.Registry = &resourcemgr.ManagerFactory{}
	resourcemgr.Registry.Register(resourcemgr.OptimusType, func(conf interface{}) (resourcemgr.ResourceManager, error) {
		return manager, nil
	})

	conf := config.ResourceManager{
		Name:        "test",
		Type:        resourcemgr.OptimusType,
		Description: "config for testing",
		Config: config.ResourceManagerConfigOptimus{
			Host:    "localhost",
			Headers: map[string]string{"key": "value"},
		},
	}
	var err error
	resolver, err = job.NewOptimusDependencyResolver(conf)
	if err != nil {
		panic(err)
	}
	return resolver, manager, tearDown
}

func TestNewOptimusDependencyResolver(t *testing.T) {
	t.Run("should return nil and error if config does not follow optimus resource manager config", func(t *testing.T) {
		var conf config.ResourceManager

		actualOptimusDependencyResolver, actualError := job.NewOptimusDependencyResolver(conf)

		assert.Nil(t, actualOptimusDependencyResolver)
		assert.Error(t, actualError)
	})

	t.Run("should return nil and error if error encountered when getting optimus resource manager", func(t *testing.T) {
		originalRegistry := resourcemgr.Registry
		defer func() { resourcemgr.Registry = originalRegistry }()

		resourcemgr.Registry = &resourcemgr.ManagerFactory{}
		resourcemgr.Registry.Register(resourcemgr.OptimusType, func(conf interface{}) (resourcemgr.ResourceManager, error) {
			return nil, errors.New("random error")
		})

		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: "localhost",
			},
		}

		actualOptimusDependencyResolver, actualError := job.NewOptimusDependencyResolver(conf)

		assert.Nil(t, actualOptimusDependencyResolver)
		assert.Error(t, actualError)
	})

	t.Run("should return optimus dependency resolver and nil if no error is encountered", func(t *testing.T) {
		originalRegistry := resourcemgr.Registry
		defer func() { resourcemgr.Registry = originalRegistry }()

		resourcemgr.Registry = &resourcemgr.ManagerFactory{}
		resourcemgr.Registry.Register(resourcemgr.OptimusType, func(conf interface{}) (resourcemgr.ResourceManager, error) {
			return mock.NewResourceManager(t), nil
		})

		conf := config.ResourceManager{
			Config: config.ResourceManagerConfigOptimus{
				Host: "localhost",
			},
		}

		actualOptimusDependencyResolver, actualError := job.NewOptimusDependencyResolver(conf)

		assert.NotNil(t, actualOptimusDependencyResolver)
		assert.NoError(t, actualError)
	})
}

func TestOptimusDependencyResolver(t *testing.T) {
	suite.Run(t, &OptimusDependencyResolverTestSuite{})
}