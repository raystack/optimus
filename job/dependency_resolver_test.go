package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func TestDependencyResolver(t *testing.T) {
	t.Run("Resolve", func(t *testing.T) {
		ctx := context.Background()
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			Name: projectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}

		namespaceSpec := models.NamespaceSpec{
			Name: "namespace-123",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}

		t.Run("it should resolve runtime dependencies", func(t *testing.T) {
			execUnit1 := new(mock.DependencyResolverMod)
			defer execUnit1.AssertExpectations(t)

			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit1},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
				Hooks: []models.JobSpecHook{
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit1},
						DependsOn: nil,
					},
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit2},
						DependsOn: nil,
					},
				},
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit1},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			// task dependencies
			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
			assert.Equal(t, []*models.JobSpecHook{&resolvedJobSpec1.Hooks[0]}, resolvedJobSpec1.Hooks[1].DependsOn)
		})
		t.Run("it should resolve runtime dependencies when more than 1 destination are found for a job giving higher priority to same project", func(t *testing.T) {
			execUnit1 := new(mock.DependencyResolverMod)
			defer execUnit1.AssertExpectations(t)

			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit1},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
				Hooks: []models.JobSpecHook{
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit1},
						DependsOn: nil,
					},
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit2},
						DependsOn: nil,
					},
				},
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit1},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{
				{
					Project: models.ProjectSpec{
						Name: "different-proj",
					},
					Job: jobSpec2,
				},
				{
					Project: projectSpec, Job: jobSpec2,
				},
			}, nil)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.tablex_destination").Return(nil, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			// task dependencies
			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{
					"project.dataset.tablex_destination",
					"project.dataset.table2_destination",
				},
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
			assert.Equal(t, []*models.JobSpecHook{&resolvedJobSpec1.Hooks[0]}, resolvedJobSpec1.Hooks[1].DependsOn)
		})
		t.Run("it should resolve runtime dependencies when more than 1 destination are found for a job to choose any random if none belong to current project", func(t *testing.T) {
			execUnit1 := new(mock.DependencyResolverMod)
			defer execUnit1.AssertExpectations(t)

			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit1},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
				Hooks: []models.JobSpecHook{
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit1},
						DependsOn: nil,
					},
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit2},
						DependsOn: nil,
					},
				},
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit1},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{
				{
					Project: models.ProjectSpec{
						Name: "different-proj",
					},
					Job: jobSpec2,
				},
				{
					Project: models.ProjectSpec{
						Name: "different-proj",
					}, Job: jobSpec2,
				},
			}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			// task dependencies
			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &models.ProjectSpec{
					Name: "different-proj",
				}, Type: models.JobSpecDependencyTypeInter},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
			assert.Equal(t, []*models.JobSpecHook{&resolvedJobSpec1.Hooks[0]}, resolvedJobSpec1.Hooks[1].DependsOn)
		})
		t.Run("it should resolve all dependencies including static unresolved dependency", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec3 := models.JobSpec{
				Version: 1,
				Name:    "test3",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baa",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies:  map[string]models.JobSpecDependency{"test3": {Job: &jobSpec3, Type: models.JobSpecDependencyTypeIntra}},
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"},
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
				jobSpec3.Name: {Job: &jobSpec3, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
		})

		t.Run("should fail if GetByDestination fails", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, errors.New("random error"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(
				&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)

			assert.Error(t, fmt.Errorf(job.UnknownRuntimeDependencyMessage,
				"project.dataset.table2_destination: %w", jobSpec1.Name, errors.New("random error")),
				err.Error())
			assert.Equal(t, models.JobSpec{}, resolvedJobSpec1)
		})

		t.Run("should fail if GenerateDependencies fails", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, errors.New("random error"))
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)

			assert.Equal(t, "random error", err.Error())
			assert.Equal(t, models.JobSpec{}, resolvedJobSpec1)
		})

		t.Run("should fail if job destination is undefined", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table3_destination").Return([]store.ProjectJobPair{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Error(t, fmt.Errorf(job.UnknownRuntimeDependencyMessage,
				"project.dataset.table3_destination", jobSpec1.Name),
				err.Error(), errors.New("spec not found"))
		})

		t.Run("it should fail for unknown static dependency", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  map[string]models.JobSpecDependency{"static_dep": {Job: nil, Type: models.JobSpecDependencyTypeIntra}},
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table1_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec1,
			}}, nil)
			jobSpecRepository.On("GetByName", ctx, "static_dep").Return(nil, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table1_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Equal(t, "unknown local dependency for job static_dep: spec not found", err.Error())
		})

		t.Run("it should fail for unknown static dependency type", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  map[string]models.JobSpecDependency{"static_dep": {Job: nil, Type: "bad"}},
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table1_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec1,
			}}, nil)
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table1_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Equal(t, "unsupported dependency type: bad", err.Error())
		})

		t.Run("it should resolve any unresolved intra static dependency", func(t *testing.T) {
			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec3 := models.JobSpec{
				Version: 1,
				Name:    "test3",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baa",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}
			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: nil, Type: models.JobSpecDependencyTypeIntra}},
				// explicitly setting this to nil. which should get resolved
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)
			jobSpecRepository.On("GetByName", ctx, "test3").Return(jobSpec3, namespaceSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"},
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Nil(t, err)
			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
				jobSpec3.Name: {Job: &jobSpec3, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
		})

		t.Run("it should resolve any static inter dependency and intra inferred dependency", func(t *testing.T) {
			externalProjectName := "an-external-data-project"
			externalProjectSpec := models.ProjectSpec{
				Name: externalProjectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			execUnit := new(mock.DependencyResolverMod)
			defer execUnit.AssertExpectations(t)

			jobSpec3 := models.JobSpec{
				Version: 1,
				Name:    "test3",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baa",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}
			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				// explicitly setting a dirty intra dependency
				Dependencies:  map[string]models.JobSpecDependency{externalProjectName + "/" + jobSpec3.Name: {Job: nil, Type: models.JobSpecDependencyTypeInter}},
				NamespaceSpec: namespaceSpec,
			}

			// destination: project.dataset.table2_destination
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: namespaceSpec,
			}

			// destination: project.dataset.table2_external_destination
			jobSpecExternal := models.JobSpec{
				Version: 1,
				Name:    "test2-external",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: execUnit},
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_external_destination").Return([]store.ProjectJobPair{{
				Project: externalProjectSpec, Job: jobSpecExternal,
			}}, nil)
			jobSpecRepository.On("GetByNameForProject", ctx, externalProjectName, "test3").Return(jobSpec3, externalProjectSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{
					"project.dataset.table2_destination",
					"project.dataset.table2_external_destination", // inter optimus dependency
				},
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil, pluginService)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Nil(t, err)
			assert.Equal(t, models.JobSpecDependency{Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra}, resolvedJobSpec1.Dependencies[jobSpec2.Name])
			assert.Equal(t, models.JobSpecDependency{Job: &jobSpecExternal, Project: &externalProjectSpec, Type: models.JobSpecDependencyTypeInter}, resolvedJobSpec1.Dependencies[jobSpecExternal.Name])
			assert.Equal(t, models.JobSpecDependency{Job: &jobSpec3, Project: &externalProjectSpec, Type: models.JobSpecDependencyTypeInter}, resolvedJobSpec1.Dependencies[externalProjectName+"/"+jobSpec3.Name])
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
		})
	})

	t.Run("Persist", func(t *testing.T) {
		ctx := context.Background()
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			Name: projectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}

		t.Run("should able to persist dependencies successfully", func(t *testing.T) {
			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecDependency := models.JobSpecDependency{
				Project: &projectSpec,
				Job:     &jobSpec2,
				Type:    models.JobSpecDependencyTypeIntra,
			}
			jobSpec1.Dependencies[jobSpec2.Name] = jobSpecDependency

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			jobDependencyRepository.On("DeleteByJobID", ctx, jobSpec1.ID).Return(nil)
			jobDependencyRepository.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpecDependency).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepository, nil)
			err := resolver.Persist(ctx, jobSpec1)

			assert.Nil(t, err)
		})

		t.Run("should return fail when failed to delete dependencies", func(t *testing.T) {
			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			errorMsg := "internal error"
			jobDependencyRepository.On("DeleteByJobID", ctx, jobSpec1.ID).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepository, nil)
			err := resolver.Persist(ctx, jobSpec1)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should return fail when failed to save dependency", func(t *testing.T) {
			jobSpec1 := models.JobSpec{
				Version: 1,
				Name:    "test1",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecDependency := models.JobSpecDependency{
				Project: &projectSpec,
				Job:     &jobSpec2,
				Type:    models.JobSpecDependencyTypeIntra,
			}
			jobSpec1.Dependencies[jobSpec2.Name] = jobSpecDependency

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			jobDependencyRepository.On("DeleteByJobID", ctx, jobSpec1.ID).Return(nil)
			errorMsg := "internal error"
			jobDependencyRepository.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpecDependency).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepository, nil)
			err := resolver.Persist(ctx, jobSpec1)

			assert.Equal(t, errorMsg, err.Error())
		})
	})

	t.Run("FetchJobSpecsWithJobDependencies", func(t *testing.T) {
		ctx := context.Background()
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "a-data-project",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		externalProjectSpec1 := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "b-data-project",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		externalProjectSpec2 := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "b-data-project",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		errorMsg := "internal error"

		t.Run("should able to fetch job specs with job dependencies", func(t *testing.T) {
			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version:      1,
				Name:         "test1",
				ID:           uuid.New(),
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version:      1,
				Name:         "test2",
				ID:           uuid.New(),
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec3 := models.JobSpec{
				Version: 1,
				Name:    "test3",
				ID:      uuid.New(),
			}
			jobSpec4 := models.JobSpec{
				Version: 1,
				Name:    "test4",
				ID:      uuid.New(),
			}
			jobSpec5 := models.JobSpec{
				Version: 1,
				Name:    "test5",
				ID:      uuid.New(),
			}
			jobSpecsBase := []models.JobSpec{jobSpec1, jobSpec2}

			persistedDependencies := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec2.ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec3.ID,
					DependentProject: externalProjectSpec1,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobSpec2.ID,
					DependentJobID:   jobSpec4.ID,
					DependentProject: externalProjectSpec2,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobSpec2.ID,
					DependentJobID:   jobSpec5.ID,
					DependentProject: externalProjectSpec2,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}
			expectedEnrichedSpecs := []models.JobSpec{
				{
					Version: 1,
					Name:    jobSpec1.Name,
					ID:      jobSpec1.ID,
					Dependencies: map[string]models.JobSpecDependency{
						jobSpec2.Name: {
							Project: &projectSpec,
							Job:     &jobSpec2,
							Type:    models.JobSpecDependencyTypeIntra,
						},
						jobSpec3.Name: {
							Project: &externalProjectSpec1,
							Job:     &jobSpec3,
							Type:    models.JobSpecDependencyTypeInter,
						},
					},
				},
				{
					Version: 1,
					Name:    jobSpec2.Name,
					ID:      jobSpec2.ID,
					Dependencies: map[string]models.JobSpecDependency{
						jobSpec4.Name: {
							Project: &externalProjectSpec1,
							Job:     &jobSpec4,
							Type:    models.JobSpecDependencyTypeInter,
						},
						jobSpec5.Name: {
							Project: &externalProjectSpec2,
							Job:     &jobSpec5,
							Type:    models.JobSpecDependencyTypeInter,
						},
					},
				},
			}

			// fetch all jobs in a project
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			// fetch dependencies of a project
			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return(persistedDependencies, nil)

			// fetch job specs of external project dependency
			projJobSpecRepoFac.On("New", externalProjectSpec1).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetByIDs", ctx, []uuid.UUID{jobSpec3.ID}).Return([]models.JobSpec{jobSpec3}, nil)
			projJobSpecRepoFac.On("New", externalProjectSpec2).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetByIDs", ctx, []uuid.UUID{jobSpec4.ID, jobSpec5.ID}).Return([]models.JobSpec{jobSpec4, jobSpec5}, nil)

			resolver := job.NewDependencyResolver(projJobSpecRepoFac, jobDependencyRepository, nil)
			actual, err := resolver.FetchJobSpecsWithJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, err)
			assert.Equal(t, []uuid.UUID{expectedEnrichedSpecs[0].ID, expectedEnrichedSpecs[1].ID}, []uuid.UUID{actual[0].ID, actual[1].ID})
			assert.Equal(t, len(expectedEnrichedSpecs[0].Dependencies), len(actual[0].Dependencies))
			assert.Equal(t, len(expectedEnrichedSpecs[1].Dependencies), len(actual[1].Dependencies))
		})
		t.Run("should failed when unable to get job specs", func(t *testing.T) {
			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// fetch all jobs in a project
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projJobSpecRepoFac, jobDependencyRepository, nil)
			actual, err := resolver.FetchJobSpecsWithJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, actual)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should failed when unable to get persisted dependencies", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version:      1,
				Name:         "test1",
				ID:           uuid.New(),
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version:      1,
				Name:         "test2",
				ID:           uuid.New(),
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecsBase := []models.JobSpec{jobSpec1, jobSpec2}

			// fetch all jobs in a project
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return([]models.JobIDDependenciesPair{}, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projJobSpecRepoFac, jobDependencyRepository, nil)
			actual, err := resolver.FetchJobSpecsWithJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, actual)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should failed if unable to get external job specs by IDs", func(t *testing.T) {
			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				Version:      1,
				Name:         "test1",
				ID:           uuid.New(),
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version:      1,
				Name:         "test2",
				ID:           uuid.New(),
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec3 := models.JobSpec{
				Version: 1,
				Name:    "test3",
				ID:      uuid.New(),
			}
			jobSpec4 := models.JobSpec{
				Version: 1,
				Name:    "test4",
				ID:      uuid.New(),
			}
			jobSpec5 := models.JobSpec{
				Version: 1,
				Name:    "test5",
				ID:      uuid.New(),
			}
			jobSpecsBase := []models.JobSpec{jobSpec1, jobSpec2}

			persistedDependencies := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec2.ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec3.ID,
					DependentProject: externalProjectSpec1,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobSpec2.ID,
					DependentJobID:   jobSpec4.ID,
					DependentProject: externalProjectSpec2,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobSpec2.ID,
					DependentJobID:   jobSpec5.ID,
					DependentProject: externalProjectSpec2,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}

			// fetch all jobs in a project
			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			// fetch dependencies of a project
			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return(persistedDependencies, nil)

			// fetch job specs of external project dependency
			projJobSpecRepoFac.On("New", externalProjectSpec1).Return(projectJobSpecRepo).Maybe()
			projectJobSpecRepo.On("GetByIDs", ctx, []uuid.UUID{jobSpec3.ID}).Return([]models.JobSpec{jobSpec3}, nil).Maybe()

			projJobSpecRepoFac.On("New", externalProjectSpec2).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetByIDs", ctx, []uuid.UUID{jobSpec4.ID, jobSpec5.ID}).Return([]models.JobSpec{}, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projJobSpecRepoFac, jobDependencyRepository, nil)
			actual, err := resolver.FetchJobSpecsWithJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, actual)
			assert.Equal(t, errorMsg, err.Error())
		})
	})

	t.Run("FetchHookWithDependencies", func(t *testing.T) {
		t.Run("should able to return hooks with resolved dependency", func(t *testing.T) {
			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

			jobSpec := models.JobSpec{
				Version:      1,
				Name:         "test1",
				Owner:        "optimus",
				Dependencies: make(map[string]models.JobSpecDependency),
				Hooks: []models.JobSpecHook{
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit1},
						DependsOn: nil,
					},
					{
						Config:    nil,
						Unit:      &models.Plugin{Base: hookUnit2},
						DependsOn: nil,
					},
				},
			}
			expectedHooks := []models.JobSpecHook{
				{
					Config:    nil,
					Unit:      &models.Plugin{Base: hookUnit1},
					DependsOn: nil,
				},
				{
					Config: nil,
					Unit:   &models.Plugin{Base: hookUnit2},
					DependsOn: []*models.JobSpecHook{
						{
							Config:    nil,
							Unit:      &models.Plugin{Base: hookUnit1},
							DependsOn: nil,
						},
					},
				},
			}

			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{Name: "hook1"}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{Name: "hook2", DependsOn: []string{"hook1"}}, nil)

			resolver := job.NewDependencyResolver(nil, nil, nil)
			actual := resolver.FetchHookWithDependencies(jobSpec)

			assert.Equal(t, expectedHooks, actual)
		})
	})
}
