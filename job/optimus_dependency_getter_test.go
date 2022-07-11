package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type OptimusDependencyGetterTestSuite struct {
	suite.Suite
}

func (o *OptimusDependencyGetterTestSuite) TestGetOptimusDependencies() {
	o.Run("should return nil and error if context is nil", func() {
		resourceManagerName := "optimus"
		resourceManagerConfigOptimus := config.ResourceManagerConfigOptimus{}
		resourceManager := mock.NewResourceManager(o.T())
		dependencyGetter := job.NewTestOptimusDependencyGetter(resourceManagerName, resourceManagerConfigOptimus, resourceManager)

		var ctx context.Context
		filter := models.JobSpecFilter{}

		actualDependencies, actualError := dependencyGetter.GetOptimusDependencies(ctx, filter)

		o.Nil(actualDependencies)
		o.Error(actualError)
	})

	o.Run("should return nil and error if error when getting job specifications from resource manager", func() {
		resourceManagerName := "optimus"
		resourceManagerConfigOptimus := config.ResourceManagerConfigOptimus{}
		resourceManager := mock.NewResourceManager(o.T())
		dependencyGetter := job.NewTestOptimusDependencyGetter(resourceManagerName, resourceManagerConfigOptimus, resourceManager)

		ctx := context.Background()
		filter := models.JobSpecFilter{}

		resourceManager.On("GetJobSpecifications", ctx, filter).Return(nil, errors.New("random error"))

		actualDependencies, actualError := dependencyGetter.GetOptimusDependencies(ctx, filter)

		o.Nil(actualDependencies)
		o.Error(actualError)
	})

	o.Run("should return optimus dependencies and nil if no error is encountered", func() {
		resourceManagerName := "optimus"
		resourceManagerConfigOptimus := config.ResourceManagerConfigOptimus{
			Host:    "localhost",
			Headers: map[string]string{"key": "value"},
		}
		resourceManager := mock.NewResourceManager(o.T())
		dependencyGetter := job.NewTestOptimusDependencyGetter(resourceManagerName, resourceManagerConfigOptimus, resourceManager)

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
		resourceManager.On("GetJobSpecifications", ctx, filter).Return(jobSpecs, nil)

		expectedDependencies := []models.OptimusDependency{
			{
				Name:          "optimus",
				Host:          "localhost",
				Headers:       map[string]string{"key": "value"},
				ProjectName:   "project",
				NamespaceName: "namespace",
				JobName:       "job",
			},
		}

		actualDependencies, actualError := dependencyGetter.GetOptimusDependencies(ctx, filter)

		o.EqualValues(expectedDependencies, actualDependencies)
		o.NoError(actualError)
	})
}

func TestOptimusDependencyResolver(t *testing.T) {
	suite.Run(t, &OptimusDependencyGetterTestSuite{})
}
