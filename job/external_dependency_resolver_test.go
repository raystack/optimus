package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type ExternalDependencyResolverTestSuite struct {
	suite.Suite
}

func (e *ExternalDependencyResolverTestSuite) TestFetchInferredExternalDependenciesPerJobName() {
	e.Run("should return nil and error if context is nil", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		var ctx context.Context
		projectID := models.ProjectID(uuid.New())

		actualDependencies, actualError := externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualDependencies)
		e.Error(actualError)
	})

	e.Run("should return nil and error if error is encountered when getting inferred dependency urns per job name", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownJobDependencyRepository.On("GetUnknownInferredDependencyURNsPerJobName", ctx, projectID).Return(nil, errors.New("random error"))

		actualDependencies, actualError := externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualDependencies)
		e.Error(actualError)
	})

	// TODO: this unit test need to be changed if the dependency is not from Optimus only, but currently it is
	e.Run("should return nil and error if error when fetching optimus dependencies from optimus dependency getter", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownInferredDependenciesPerJobName := map[string][]string{"job1": {"urn1"}}
		unknownJobDependencyRepository.On("GetUnknownInferredDependencyURNsPerJobName", ctx, projectID).Return(unknownInferredDependenciesPerJobName, nil)

		filter1 := models.JobSpecFilter{ResourceDestination: "urn1"}
		optimusResourceManager.On("GetOptimusDependencies", ctx, filter1).Return(nil, errors.New("random error"))

		actualDependencies, actualError := externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualDependencies)
		e.Error(actualError)
	})

	e.Run("should return external dependency and nil if no error is encountered", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownInferredDependenciesPerJobName := map[string][]string{"job1": {"urn1"}}
		unknownJobDependencyRepository.On("GetUnknownInferredDependencyURNsPerJobName", ctx, projectID).Return(unknownInferredDependenciesPerJobName, nil)

		filter1 := models.JobSpecFilter{ResourceDestination: "urn1"}
		optimusDependencies := []models.OptimusDependency{
			{
				Name:          "optimus",
				Host:          "localhost",
				Headers:       map[string]string{"key": "value"},
				ProjectName:   "project",
				NamespaceName: "namespace",
				JobName:       "job",
			},
		}
		optimusResourceManager.On("GetOptimusDependencies", ctx, filter1).Return(optimusDependencies, nil)

		expectedDependencies := map[string]models.ExternalDependency{
			"job1": {
				OptimusDependencies: optimusDependencies,
			},
		}

		actualDependencies, actualError := externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, projectID)

		e.EqualValues(expectedDependencies, actualDependencies)
		e.NoError(actualError)
	})
}

func (e *ExternalDependencyResolverTestSuite) TestFetchStaticExternalDependenciesPerJobName() {
	e.Run("should return nil, nil and error if context is nil", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		var ctx context.Context
		projectID := models.ProjectID(uuid.New())

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualExternalDependencies)
		e.Nil(actualUnknownDependencies)
		e.Error(actualError)
	})

	e.Run("should return nil, nil and error if error is encountered when getting static dependency names per job name", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownJobDependencyRepository.On("GetUnknownStaticDependencyNamesPerJobName", ctx, projectID).Return(nil, errors.New("random error"))

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualExternalDependencies)
		e.Nil(actualUnknownDependencies)
		e.Error(actualError)
	})

	e.Run("should return nil, nil and error if one or more static dependencies are invalid", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownStaticDependenciesPerJobName := map[string][]string{"job1": {"job2"}}
		unknownJobDependencyRepository.On("GetUnknownStaticDependencyNamesPerJobName", ctx, projectID).Return(unknownStaticDependenciesPerJobName, nil)

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualExternalDependencies)
		e.Nil(actualUnknownDependencies)
		e.Error(actualError)
	})

	// TODO: this unit test need to be changed if the dependency is not from Optimus only, but currently it is
	e.Run("should return nil, nil and error if error when fetching optimus dependencies from optimus dependency getter", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownStaticDependenciesPerJobName := map[string][]string{"job1": {"project2/job2"}}
		unknownJobDependencyRepository.On("GetUnknownStaticDependencyNamesPerJobName", ctx, projectID).Return(unknownStaticDependenciesPerJobName, nil)

		filter1 := models.JobSpecFilter{ProjectName: "project2", JobName: "job2"}
		optimusResourceManager.On("GetOptimusDependencies", ctx, filter1).Return(nil, errors.New("random error"))

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, projectID)

		e.Nil(actualExternalDependencies)
		e.Nil(actualUnknownDependencies)
		e.Error(actualError)
	})

	e.Run("should return external dependency and nil if no error is encountered", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		unknownJobDependencyRepository := mock.NewUnknownJobDependencyRepository(e.T())
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers, unknownJobDependencyRepository)

		ctx := context.Background()
		projectID := models.ProjectID(uuid.New())

		unknownStaticDependenciesPerJobName := map[string][]string{"job1": {"project2/job2", "project3/job3"}}
		unknownJobDependencyRepository.On("GetUnknownStaticDependencyNamesPerJobName", ctx, projectID).Return(unknownStaticDependenciesPerJobName, nil)

		filter1 := models.JobSpecFilter{ProjectName: "project2", JobName: "job2"}
		optimusDependencies := []models.OptimusDependency{
			{
				Name:          "optimus",
				Host:          "localhost",
				Headers:       map[string]string{"key": "value"},
				ProjectName:   "project",
				NamespaceName: "namespace",
				JobName:       "job",
			},
		}
		optimusResourceManager.On("GetOptimusDependencies", ctx, filter1).Return(optimusDependencies, nil)
		filter2 := models.JobSpecFilter{ProjectName: "project3", JobName: "job3"}
		optimusResourceManager.On("GetOptimusDependencies", ctx, filter2).Return([]models.OptimusDependency{}, nil)

		expectedDependencies := map[string]models.ExternalDependency{
			"job1": {
				OptimusDependencies: optimusDependencies,
			},
		}
		expectedUnknownDependencies := []models.UnknownDependency{{JobName: "job1", DependencyProjectName: "project3", DependencyJobName: "job3"}}

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, projectID)

		e.EqualValues(expectedDependencies, actualExternalDependencies)
		e.EqualValues(expectedUnknownDependencies, actualUnknownDependencies)
		e.NoError(actualError)
	})
}

func TestExternalDependencyResolver(t *testing.T) {
	suite.Run(t, &ExternalDependencyResolverTestSuite{})
}
