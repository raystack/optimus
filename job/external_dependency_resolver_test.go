package job_test

import (
	"context"
	"testing"

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
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers)

		var ctx context.Context
		unresolvedDependencies := []models.UnresolvedJobDependency{
			{ProjectName: "project2", JobName: "job2"},
		}
		unresolvedDependenciesPerJobName := map[string][]models.UnresolvedJobDependency{"job1": {unresolvedDependencies[0]}}

		actualDependencies, actualError := externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, unresolvedDependenciesPerJobName)

		e.Nil(actualDependencies)
		e.Error(actualError)
	})

	e.Run("should return external dependency and nil if no error is encountered", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers)

		ctx := context.Background()
		unresolvedDependencies := []models.UnresolvedJobDependency{
			{ResourceDestination: "urn1"},
		}
		unresolvedDependenciesPerJobName := map[string][]models.UnresolvedJobDependency{"job1": {unresolvedDependencies[0]}}

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
		optimusResourceManager.On("GetOptimusDependencies", ctx, unresolvedDependencies[0]).Return(optimusDependencies, nil)

		expectedDependencies := map[string]models.ExternalDependency{
			"job1": {
				OptimusDependencies: optimusDependencies,
			},
		}

		actualDependencies, actualError := externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, unresolvedDependenciesPerJobName)

		e.EqualValues(expectedDependencies, actualDependencies)
		e.NoError(actualError)
	})
}

func (e *ExternalDependencyResolverTestSuite) TestFetchStaticExternalDependenciesPerJobName() {
	e.Run("should return nil, nil and error if context is nil", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers)

		var ctx context.Context
		unresolvedDependencies := []models.UnresolvedJobDependency{
			{ProjectName: "project2", JobName: "job2"},
		}
		unresolvedDependenciesPerJobName := map[string][]models.UnresolvedJobDependency{"job1": {unresolvedDependencies[0]}}

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, unresolvedDependenciesPerJobName)

		e.Nil(actualExternalDependencies)
		e.Nil(actualUnknownDependencies)
		e.Error(actualError)
	})

	e.Run("should return external dependency and nil if no error is encountered", func() {
		optimusResourceManager := mock.NewResourceManager(e.T())
		optimusResourceManagers := []resourcemgr.ResourceManager{optimusResourceManager}
		externalDependencyResolver := job.NewTestExternalDependencyResolver(optimusResourceManagers)

		ctx := context.Background()

		unresolvedDependencies := []models.UnresolvedJobDependency{
			{ProjectName: "project2", JobName: "job2"},
			{ProjectName: "project3", JobName: "job3"},
		}
		unresolvedDependenciesPerJobName := map[string][]models.UnresolvedJobDependency{"job1": {unresolvedDependencies[0], unresolvedDependencies[1]}}
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

		optimusResourceManager.On("GetOptimusDependencies", ctx, unresolvedDependencies[0]).Return(optimusDependencies, nil)
		optimusResourceManager.On("GetOptimusDependencies", ctx, unresolvedDependencies[1]).Return([]models.OptimusDependency{}, nil)

		expectedDependencies := map[string]models.ExternalDependency{
			"job1": {
				OptimusDependencies: optimusDependencies,
			},
		}
		expectedUnknownDependencies := []models.UnknownDependency{{JobName: "job1", DependencyProjectName: "project3", DependencyJobName: "job3"}}

		actualExternalDependencies, actualUnknownDependencies, actualError := externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, unresolvedDependenciesPerJobName)

		e.EqualValues(expectedDependencies, actualExternalDependencies)
		e.EqualValues(expectedUnknownDependencies, actualUnknownDependencies)
		e.NoError(actualError)
	})
}

func TestExternalDependencyResolver(t *testing.T) {
	suite.Run(t, &ExternalDependencyResolverTestSuite{})
}
