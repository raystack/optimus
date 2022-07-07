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
		externalProjectName := "an-external-data-project"
		externalProjectSpec := models.ProjectSpec{
			Name: externalProjectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		externalNamespaceSpec := models.NamespaceSpec{
			Name: "namespace-external",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: externalProjectSpec,
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

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, "project.dataset.table2_destination").Return(jobSpec2, nil)
			defer jobSpecRepository.AssertExpectations(t)

			// task dependencies
			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: jobSpec1Sources}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)

			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec2.ID, nil).Return(nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, "project.dataset.table2_destination").Return(jobSpec2, nil)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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

		t.Run("should fail if GetJobByResourceDestination fails", func(t *testing.T) {
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

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, "project.dataset.table2_destination").Return(jobSpec2, errors.New("random error"))
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(
				&models.GenerateDependenciesResponse{Dependencies: jobSpec1Sources}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, errors.New("random error"))
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(nil, nil, pluginService, projectJobSpecRepoFactory, nil)
			resolvedJobSpec1, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)

			assert.Equal(t, "random error", err.Error())
			assert.Equal(t, models.JobSpec{}, resolvedJobSpec1)
		})

		t.Run("should fail if unable to persist job sources", func(t *testing.T) {
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

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table3_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			errorMsg := "internal error"
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Contains(t, err.Error(), errorMsg)
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

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, "project.dataset.table3_destination").Return(models.JobSpec{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table3_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec1, nil)
			assert.Error(t, fmt.Errorf(job.UnknownRuntimeDependencyMessage,
				jobSpec1Sources[0], jobSpec1.Name),
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
			jobSpec1Sources := []string{"project.dataset.table1_destination"}

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepository.On("GetByName", ctx, "static_dep").Return(models.JobSpec{}, models.NamespaceSpec{}, errors.New("spec not found"))
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, jobSpec1Sources[0]).Return(jobSpec1, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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
			jobSpec1Sources := []string{"project.dataset.table1_destination"}

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, jobSpec1Sources[0]).Return(jobSpec1, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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
			jobSpec1Sources := []string{"project.dataset.table2_destination"}

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepository.On("GetByName", ctx, "test3").Return(jobSpec3, namespaceSpec, nil)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, jobSpec1Sources[0]).Return(jobSpec2, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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
				Dependencies:  make(map[string]models.JobSpecDependency),
				NamespaceSpec: externalNamespaceSpec,
			}

			jobSpec1Sources := []string{
				"project.dataset.table2_destination",
				"project.dataset.table2_external_destination", // inter optimus dependency
			}

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepository.On("GetByNameForProject", ctx, externalProjectName, "test3").Return(jobSpec3, externalProjectSpec, nil)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, jobSpec1Sources[0]).Return(jobSpec2, nil)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, jobSpec1Sources[1]).Return(jobSpecExternal, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, projectJobSpecRepoFactory, nil)
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

	t.Run("ResolveStaticDependencies", func(t *testing.T) {
		t.Run("should return nil and error if context is nil", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			var ctx context.Context
			job := models.JobSpec{
				Name: "job1",
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := &mock.ProjectJobSpecRepository{}

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if job spec is empty", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			job := models.JobSpec{}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := &mock.ProjectJobSpecRepository{}

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if project spec is empty", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			job := models.JobSpec{
				Name: "job1",
			}
			project := models.ProjectSpec{}
			projectJobSpecRepo := &mock.ProjectJobSpecRepository{}

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if project job spec repo is nil", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			job := models.JobSpec{
				Name: "job1",
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			var projectJobSpecRepo store.ProjectJobSpecRepository

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if error encountered when getting job on intra dependency", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			jobDependency := models.JobSpec{
				Name: "job1_dependency",
			}
			job := models.JobSpec{
				Name: "job1",
				Dependencies: map[string]models.JobSpecDependency{
					"job1_dependency": {
						Type: models.JobSpecDependencyTypeIntra,
					},
				},
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := mock.NewProjectJobSpecRepository(t)

			projectJobSpecRepo.On("GetByName", ctx, "job1_dependency").Return(jobDependency, models.NamespaceSpec{}, errors.New("random error"))

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if error encountered when extracting project and job on inter dependency", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			job := models.JobSpec{
				Name: "job1",
				Dependencies: map[string]models.JobSpecDependency{
					"job1_dependency": {
						Type: models.JobSpecDependencyTypeInter,
					},
				},
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := mock.NewProjectJobSpecRepository(t)

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if error encountered when getting job on inter dependency", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			jobDependency := models.JobSpec{
				Name: "job1_dependency",
			}
			job := models.JobSpec{
				Name: "job1",
				Dependencies: map[string]models.JobSpecDependency{
					"project1/job1_dependency": {
						Type: models.JobSpecDependencyTypeInter,
					},
				},
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := mock.NewProjectJobSpecRepository(t)

			projectJobSpecRepo.On("GetByNameForProject", ctx, "project1", "job1_dependency").Return(jobDependency, models.ProjectSpec{}, errors.New("random error"))

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if dependency type is unrecognized", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			job := models.JobSpec{
				Name: "job1",
				Dependencies: map[string]models.JobSpecDependency{
					"project1/job1_dependency": {
						Type: "unknown",
					},
				},
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := mock.NewProjectJobSpecRepository(t)

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.Nil(t, actualDependencies)
			assert.Error(t, actualError)
		})

		t.Run("should return dependency and nil if no error is encountered", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, projectJobFactory, nil)

			ctx := context.Background()
			jobDependency1 := models.JobSpec{
				Name: "job1_dependency1",
			}
			jobDependency2 := models.JobSpec{
				Name: "job1_dependency2",
			}
			job := models.JobSpec{
				Name: "job1",
				Dependencies: map[string]models.JobSpecDependency{
					"job1_dependency1": {
						Type: models.JobSpecDependencyTypeIntra,
					},
					"project1/job1_dependency2": {
						Type: models.JobSpecDependencyTypeInter,
					},
				},
			}
			project := models.ProjectSpec{
				Name: "project1",
			}
			projectJobSpecRepo := mock.NewProjectJobSpecRepository(t)

			projectJobSpecRepo.On("GetByName", ctx, "job1_dependency1").Return(jobDependency1, models.NamespaceSpec{}, nil)
			projectJobSpecRepo.On("GetByNameForProject", ctx, "project1", "job1_dependency2").Return(jobDependency2, models.ProjectSpec{}, nil)

			actualDependencies, actualError := dependencyResolver.ResolveStaticDependencies(ctx, job, project, projectJobSpecRepo)

			assert.NotNil(t, actualDependencies)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetJobSpecsWithDependencies", func(t *testing.T) {
		t.Run("return nil and error if context is nil", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, nil)

			var ctx context.Context
			projectID := models.ProjectID(uuid.New())

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting all job specs within the project", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, nil)

			ctx := context.Background()
			projectID := models.ProjectID(uuid.New())

			jobSpecRepo.On("GetAllByProjectID", ctx, projectID).Return(nil, errors.New("random error"))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting static dependencies per job", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, nil)

			ctx := context.Background()
			projectID := models.ProjectID(uuid.New())

			jobSpecRepo.On("GetAllByProjectID", ctx, projectID).Return([]models.JobSpec{}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectID).Return(nil, errors.New("random error"))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting inferred dependencies per job", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, nil)

			ctx := context.Background()
			projectID := models.ProjectID(uuid.New())

			jobSpecRepo.On("GetAllByProjectID", ctx, projectID).Return([]models.JobSpec{}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectID).Return(map[uuid.UUID][]models.JobSpec{}, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectID).Return(nil, errors.New("random error"))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error when unable to fetch static external dependencies", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			basePlugin := &mock.BasePlugin{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, externalDependencyResolver)

			ctx := context.Background()
			projectID := models.ProjectID(uuid.New())

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "project",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "namespace",
				ProjectSpec: projectSpec,
			}
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "job",
				Hooks: []models.JobSpecHook{
					{
						Unit: &models.Plugin{
							Base: basePlugin,
						},
					},
				},
			}
			staticDependencies := map[uuid.UUID][]models.JobSpec{
				jobSpec.ID: {
					{
						Name:          "job-a",
						NamespaceSpec: namespaceSpec,
					},
				},
			}
			inferredDependencies := map[uuid.UUID][]models.JobSpec{
				jobSpec.ID: {
					{
						Name:          "job-b",
						NamespaceSpec: namespaceSpec,
					},
				},
			}

			basePlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectID", ctx, projectID).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectID).Return(staticDependencies, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectID).Return(inferredDependencies, nil)

			errorMsg := "internal error"
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, projectID).Return(nil, nil, errors.New(errorMsg))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.Equal(t, errorMsg, actualError.Error())
			assert.Nil(t, actualJobSpecs)
		})
		t.Run("should return nil and error when unable to fetch inferred external dependencies", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			basePlugin := &mock.BasePlugin{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, externalDependencyResolver)

			ctx := context.Background()
			projectID := models.ProjectID(uuid.New())

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "project",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "namespace",
				ProjectSpec: projectSpec,
			}
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "job",
				Hooks: []models.JobSpecHook{
					{
						Unit: &models.Plugin{
							Base: basePlugin,
						},
					},
				},
			}
			staticDependencies := map[uuid.UUID][]models.JobSpec{
				jobSpec.ID: {
					{
						Name:          "job-a",
						NamespaceSpec: namespaceSpec,
					},
				},
			}
			inferredDependencies := map[uuid.UUID][]models.JobSpec{
				jobSpec.ID: {
					{
						Name:          "job-b",
						NamespaceSpec: namespaceSpec,
					},
				},
			}

			basePlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectID", ctx, projectID).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectID).Return(staticDependencies, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectID).Return(inferredDependencies, nil)

			errorMsg := "internal error"
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, projectID).Return(nil, nil, nil)
			externalDependencyResolver.On("FetchInferredExternalDependenciesPerJobName", ctx, projectID).Return(nil, errors.New(errorMsg))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.Equal(t, errorMsg, actualError.Error())
			assert.Nil(t, actualJobSpecs)
		})
		t.Run("return job specs with their external dependencies info and nil if no error is encountered", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			basePlugin := &mock.BasePlugin{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil, externalDependencyResolver)

			ctx := context.Background()
			projectID := models.ProjectID(uuid.New())

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "project",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "namespace",
				ProjectSpec: projectSpec,
			}
			jobSpec := models.JobSpec{
				ID:   uuid.New(),
				Name: "job",
				Hooks: []models.JobSpecHook{
					{
						Unit: &models.Plugin{
							Base: basePlugin,
						},
					},
				},
			}
			basePlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectID", ctx, projectID).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectID).Return(nil, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectID).Return(nil, nil)

			staticExternalDependencies := map[string]models.ExternalDependency{
				jobSpec.Name: {OptimusDependencies: []models.OptimusDependency{
					{
						Name:          "optimus-server-1",
						Host:          "host-1",
						ProjectName:   projectSpec.Name,
						NamespaceName: namespaceSpec.Name,
						JobName:       "external-dependency-1",
					},
				}},
			}
			inferredExternalDependencies := map[string]models.ExternalDependency{
				jobSpec.Name: {OptimusDependencies: []models.OptimusDependency{
					{
						Name:          "optimus-server-1",
						Host:          "host-1",
						ProjectName:   projectSpec.Name,
						NamespaceName: namespaceSpec.Name,
						JobName:       "external-dependency-2",
					},
				}},
			}
			unknownDependencies := []models.UnknownDependency{
				{
					JobName:               jobSpec.Name,
					DependencyProjectName: "unknown-project-1",
					DependencyJobName:     "unknown-job-1",
				},
			}
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, projectID).Return(staticExternalDependencies, unknownDependencies, nil)
			externalDependencyResolver.On("FetchInferredExternalDependenciesPerJobName", ctx, projectID).Return(inferredExternalDependencies, nil)

			expectedJobSpecs := []models.JobSpec{
				{
					ID:   jobSpec.ID,
					Name: "job",
					Hooks: []models.JobSpecHook{
						{
							Unit: &models.Plugin{
								Base: basePlugin,
							},
							DependsOn: []*models.JobSpecHook{
								{
									Unit: &models.Plugin{
										Base: basePlugin,
									},
								},
							},
						},
					},
					Dependencies: make(map[string]models.JobSpecDependency),
					ExternalDependencies: models.ExternalDependency{OptimusDependencies: []models.OptimusDependency{
						staticExternalDependencies[jobSpec.Name].OptimusDependencies[0],
						inferredExternalDependencies[jobSpec.Name].OptimusDependencies[0],
					}},
				},
			}

			actualJobSpecs, actualUnknownDependencies, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectID)

			assert.EqualValues(t, expectedJobSpecs, actualJobSpecs)
			assert.Equal(t, unknownDependencies, actualUnknownDependencies)
			assert.NoError(t, actualError)
		})
	})
}
