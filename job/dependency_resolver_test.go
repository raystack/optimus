package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
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

			hookUnit1 := new(mock.YamlMod)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.YamlMod)
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
						Unit:      &models.Plugin{YamlMod: hookUnit1},
						DependsOn: nil,
					},
					{
						Config:    nil,
						Unit:      &models.Plugin{YamlMod: hookUnit2},
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

			jobSpec1Sources := []string{"project.dataset.table2_destination"}

			// task dependencies
			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: jobSpec1Sources}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec2.ID, nil).Return(nil)

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, jobSpec1Sources[0], false).Return([]models.JobSpec{jobSpec2}, nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, "project.dataset.table2_destination", false).Return([]models.JobSpec{jobSpec2}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
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

		t.Run("should fail if GetByResourceDestinationURN fails", func(t *testing.T) {
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, "project.dataset.table2_destination", false).Return([]models.JobSpec{jobSpec2}, errors.New("random error"))
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(
				&models.GenerateDependenciesResponse{Dependencies: jobSpec1Sources}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
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

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, errors.New("random error"))
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(nil, nil, pluginService, nil)
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

			pluginService := mock.NewPluginService(t)
			jobSpec1Sources := []string{"project.dataset.table3_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			errorMsg := "internal error"
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(nil, jobSourceRepo, pluginService, nil)
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, "project.dataset.table3_destination", false).Return([]models.JobSpec{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			jobSpec1Sources := []string{"project.dataset.table3_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, jobSpec1Sources[0], false).Return([]models.JobSpec{jobSpec1}, nil)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, "static_dep", projectName, false).Return(models.JobSpec{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)

			assert.Equal(t, multierror.Append(nil, errors.New("unknown local dependency for job static_dep: spec not found")).Error(), err.Error())
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, jobSpec1Sources[0], false).Return([]models.JobSpec{jobSpec1}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
			_, err := resolver.Resolve(ctx, projectSpec, jobSpec2, nil)
			errExpected := multierror.Append(nil, errors.New("unsupported dependency type: bad"))
			assert.Equal(t, errExpected.Error(), err.Error())
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, jobSpec1Sources[0], false).Return([]models.JobSpec{jobSpec2}, nil)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, "test3", projectName, false).Return(jobSpec3, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
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
				NamespaceSpec: externalNamespaceSpec,
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, jobSpec1Sources[0], false).Return([]models.JobSpec{jobSpec2}, nil)
			jobSpecRepository.On("GetByResourceDestinationURN", ctx, jobSpec1Sources[1], false).Return([]models.JobSpec{jobSpecExternal}, nil)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, "test3", externalProjectName, false).Return(jobSpec3, nil)
			defer jobSpecRepository.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpec1Sources).Return(nil)

			resolver := job.NewDependencyResolver(jobSpecRepository, jobSourceRepo, pluginService, nil)
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

	t.Run("GetJobSpecsWithDependencies", func(t *testing.T) {
		t.Run("return nil and error if context is nil", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil)

			var ctx context.Context
			projectName := "project_test"

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if project name is empty", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil)

			ctx := context.Background()
			var projectName string

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting all job specs within the project", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil)

			ctx := context.Background()
			projectName := "project_test"

			jobSpecRepo.On("GetAllByProjectName", ctx, projectName, false).Return(nil, errors.New("random error"))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting static dependencies per job", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil)

			ctx := context.Background()
			projectName := "project_test"

			jobSpecRepo.On("GetAllByProjectName", ctx, projectName, false).Return([]models.JobSpec{}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectName).Return(nil, errors.New("random error"))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting inferred dependencies per job", func(t *testing.T) {
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, nil)

			ctx := context.Background()
			projectName := "project_test"

			jobSpecRepo.On("GetAllByProjectName", ctx, projectName, false).Return([]models.JobSpec{}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectName).Return(map[uuid.UUID][]models.JobSpec{}, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectName).Return(nil, errors.New("random error"))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("should return nil, nil and error when unable to fetch static external dependencies", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			yamlPlugin := &mock.YamlMod{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, nil, nil, externalDependencyResolver)

			ctx := context.Background()
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
							YamlMod: yamlPlugin,
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
			jobSpec.Dependencies = map[string]models.JobSpecDependency{
				"job-a":                         {},
				"external-project/external-job": {},
			}
			inferredDependencies := map[uuid.UUID][]models.JobSpec{
				jobSpec.ID: {
					{
						Name:          "job-b",
						NamespaceSpec: namespaceSpec,
					},
				},
			}

			yamlPlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectName", ctx, projectSpec.Name, false).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectSpec.Name).Return(staticDependencies, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectSpec.Name).Return(inferredDependencies, nil)

			unresolvedJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{
				jobSpec.Name: {models.UnresolvedJobDependency{
					ProjectName: "external-project",
					JobName:     "external-job",
				}},
			}

			errorMsg := "internal error"
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, unresolvedJobDependencyPerJobName).Return(nil, nil, errors.New(errorMsg))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectSpec.Name)

			assert.Equal(t, errorMsg, actualError.Error())
			assert.Nil(t, actualJobSpecs)
		})

		t.Run("should return nil and error when unable to fetch job source URNs", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.AssertExpectations(t)

			yamlPlugin := &mock.YamlMod{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, jobSourceRepo, nil, externalDependencyResolver)

			ctx := context.Background()
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
							YamlMod: yamlPlugin,
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

			yamlPlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectName", ctx, projectSpec.Name, false).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectSpec.Name).Return(staticDependencies, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectSpec.Name).Return(inferredDependencies, nil)

			unresolvedJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{}

			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, unresolvedJobDependencyPerJobName).Return(map[string]models.ExternalDependency{}, nil, nil)

			errorMsg := "internal error"
			jobSourceRepo.On("GetResourceURNsPerJobID", ctx, projectSpec.Name).Return(nil, errors.New(errorMsg))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectSpec.Name)

			assert.Equal(t, errorMsg, actualError.Error())
			assert.Nil(t, actualJobSpecs)
		})

		t.Run("should return nil, nil and error when unable to fetch inferred external dependencies", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.AssertExpectations(t)

			yamlPlugin := &mock.YamlMod{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, jobSourceRepo, nil, externalDependencyResolver)

			ctx := context.Background()
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
							YamlMod: yamlPlugin,
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
						Name:                "job-b",
						ResourceDestination: "resource-b",
						NamespaceSpec:       namespaceSpec,
					},
				},
			}
			resourceURNsPerJobID := map[uuid.UUID][]string{
				jobSpec.ID: {"resource-b", "resource-external"},
			}

			yamlPlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectName", ctx, projectSpec.Name, false).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectSpec.Name).Return(staticDependencies, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectSpec.Name).Return(inferredDependencies, nil)

			unresolvedStaticJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{}
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, unresolvedStaticJobDependencyPerJobName).Return(map[string]models.ExternalDependency{}, nil, nil)

			jobSourceRepo.On("GetResourceURNsPerJobID", ctx, projectSpec.Name).Return(resourceURNsPerJobID, nil)

			errorMsg := "internal error"
			unresolvedInferredJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{
				jobSpec.Name: {models.UnresolvedJobDependency{ResourceDestination: "resource-external"}},
			}
			externalDependencyResolver.On("FetchInferredExternalDependenciesPerJobName", ctx, unresolvedInferredJobDependencyPerJobName).Return(nil, errors.New(errorMsg))

			actualJobSpecs, _, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectSpec.Name)

			assert.Equal(t, errorMsg, actualError.Error())
			assert.Nil(t, actualJobSpecs)
		})

		t.Run("return job specs with their external dependencies info, unknown dependencies (intra project) and nil if no error is encountered", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.AssertExpectations(t)

			yamlPlugin := &mock.YamlMod{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, jobSourceRepo, nil, externalDependencyResolver)

			ctx := context.Background()
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
							YamlMod: yamlPlugin,
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{
					"external-project/external-dependency-1": {},
					"unknown-job-1":                          {},
				},
				NamespaceSpec: namespaceSpec,
			}
			yamlPlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectName", ctx, projectSpec.Name, false).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectSpec.Name).Return(nil, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectSpec.Name).Return(nil, nil)

			staticExternalDependencies := map[string]models.ExternalDependency{
				jobSpec.Name: {OptimusDependencies: []models.OptimusDependency{
					{
						Name:          "optimus-server-1",
						Host:          "host-1",
						ProjectName:   "external-project",
						NamespaceName: namespaceSpec.Name,
						JobName:       "external-dependency-1",
					},
				}},
			}
			resourceURNsPerJobID := map[uuid.UUID][]string{
				jobSpec.ID: {"external-dependency-2"},
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
					DependencyProjectName: projectSpec.Name,
					DependencyJobName:     "unknown-job-1",
				},
			}

			unresolvedStaticJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{
				jobSpec.Name: {
					{ProjectName: "external-project", JobName: "external-dependency-1"},
				},
			}
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, unresolvedStaticJobDependencyPerJobName).Return(staticExternalDependencies, []models.UnknownDependency{}, nil)

			jobSourceRepo.On("GetResourceURNsPerJobID", ctx, projectSpec.Name).Return(resourceURNsPerJobID, nil)

			unresolvedInferredJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{
				jobSpec.Name: {models.UnresolvedJobDependency{ResourceDestination: "external-dependency-2"}},
			}
			externalDependencyResolver.On("FetchInferredExternalDependenciesPerJobName", ctx, unresolvedInferredJobDependencyPerJobName).Return(inferredExternalDependencies, nil)

			expectedJobSpecs := []models.JobSpec{
				{
					ID:   jobSpec.ID,
					Name: "job",
					Hooks: []models.JobSpecHook{
						{
							Unit: &models.Plugin{
								YamlMod: yamlPlugin,
							},
							DependsOn: []*models.JobSpecHook{
								{
									Unit: &models.Plugin{
										YamlMod: yamlPlugin,
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
					NamespaceSpec: namespaceSpec,
				},
			}

			actualJobSpecs, actualUnknownDependencies, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectSpec.Name)

			assert.EqualValues(t, expectedJobSpecs, actualJobSpecs)
			assert.Equal(t, unknownDependencies, actualUnknownDependencies)
			assert.NoError(t, actualError)
		})

		t.Run("return job specs with their external dependencies info, unknown dependencies (external) and nil if no error is encountered", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.AssertExpectations(t)

			yamlPlugin := &mock.YamlMod{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, jobSourceRepo, nil, externalDependencyResolver)

			ctx := context.Background()
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
							YamlMod: yamlPlugin,
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{
					"unknown-project-1/unknown-job-1": {},
				},
			}
			yamlPlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			jobSpecRepo.On("GetAllByProjectName", ctx, projectSpec.Name, false).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectSpec.Name).Return(nil, nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectSpec.Name).Return(nil, nil)

			staticExternalDependencies := map[string]models.ExternalDependency{}
			resourceURNsPerJobID := map[uuid.UUID][]string{
				jobSpec.ID: {"external-dependency-2"},
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

			unresolvedStaticJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{
				jobSpec.Name: {
					{ProjectName: "unknown-project-1", JobName: "unknown-job-1"},
				},
			}
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, unresolvedStaticJobDependencyPerJobName).Return(staticExternalDependencies, unknownDependencies, nil)

			jobSourceRepo.On("GetResourceURNsPerJobID", ctx, projectSpec.Name).Return(resourceURNsPerJobID, nil)

			unresolvedInferredJobDependencyPerJobName := map[string][]models.UnresolvedJobDependency{
				jobSpec.Name: {models.UnresolvedJobDependency{ResourceDestination: "external-dependency-2"}},
			}
			externalDependencyResolver.On("FetchInferredExternalDependenciesPerJobName", ctx, unresolvedInferredJobDependencyPerJobName).Return(inferredExternalDependencies, nil)

			expectedJobSpecs := []models.JobSpec{
				{
					ID:   jobSpec.ID,
					Name: "job",
					Hooks: []models.JobSpecHook{
						{
							Unit: &models.Plugin{
								YamlMod: yamlPlugin,
							},
							DependsOn: []*models.JobSpecHook{
								{
									Unit: &models.Plugin{
										YamlMod: yamlPlugin,
									},
								},
							},
						},
					},
					Dependencies: make(map[string]models.JobSpecDependency),
					ExternalDependencies: models.ExternalDependency{OptimusDependencies: []models.OptimusDependency{
						inferredExternalDependencies[jobSpec.Name].OptimusDependencies[0],
					}},
				},
			}

			actualJobSpecs, actualUnknownDependencies, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectSpec.Name)

			assert.EqualValues(t, expectedJobSpecs, actualJobSpecs)
			assert.Equal(t, unknownDependencies, actualUnknownDependencies)
			assert.NoError(t, actualError)
		})

		t.Run("return job specs with only internal dependencies, nil unknown dependencies and nil error all internal dependencies are already resolved", func(t *testing.T) {
			externalDependencyResolver := new(mock.ExternalDependencyResolver)
			externalDependencyResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.AssertExpectations(t)

			yamlPlugin := &mock.YamlMod{}
			jobSpecRepo := mock.NewJobSpecRepository(t)
			dependencyResolver := job.NewDependencyResolver(jobSpecRepo, jobSourceRepo, nil, externalDependencyResolver)

			ctx := context.Background()
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
							YamlMod: yamlPlugin,
						},
					},
				},
				NamespaceSpec: namespaceSpec,
			}
			yamlPlugin.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "plugin-c",
				DependsOn: []string{"plugin-c"},
			}, nil)

			destination := "inferred-dependency"
			inferredDependenciesPerJobID := map[uuid.UUID][]models.JobSpec{
				jobSpec.ID: {
					models.JobSpec{
						Name:                "dependency",
						ResourceDestination: destination,
						NamespaceSpec:       namespaceSpec,
					},
				},
			}

			jobSpecRepo.On("GetAllByProjectName", ctx, projectSpec.Name, false).Return([]models.JobSpec{jobSpec}, nil)
			jobSpecRepo.On("GetStaticDependenciesPerJobID", ctx, projectSpec.Name).Return(make(map[uuid.UUID][]models.JobSpec), nil)
			jobSpecRepo.On("GetInferredDependenciesPerJobID", ctx, projectSpec.Name).Return(inferredDependenciesPerJobID, nil)

			staticExternalDependencies := make(map[string]models.ExternalDependency)
			resourceURNsPerJobID := map[uuid.UUID][]string{
				jobSpec.ID: {destination},
			}
			inferredExternalDependencies := make(map[string]models.ExternalDependency)

			unresolvedStaticJobDependencyPerJobName := make(map[string][]models.UnresolvedJobDependency)
			externalDependencyResolver.On("FetchStaticExternalDependenciesPerJobName", ctx, unresolvedStaticJobDependencyPerJobName).Return(staticExternalDependencies, []models.UnknownDependency{}, nil)

			jobSourceRepo.On("GetResourceURNsPerJobID", ctx, projectSpec.Name).Return(resourceURNsPerJobID, nil)

			unresolvedInferredJobDependencyPerJobName := make(map[string][]models.UnresolvedJobDependency)
			externalDependencyResolver.On("FetchInferredExternalDependenciesPerJobName", ctx, unresolvedInferredJobDependencyPerJobName).Return(inferredExternalDependencies, nil)

			expectedJobSpecs := []models.JobSpec{
				{
					ID:   jobSpec.ID,
					Name: "job",
					Hooks: []models.JobSpecHook{
						{
							Unit: &models.Plugin{
								YamlMod: yamlPlugin,
							},
							DependsOn: []*models.JobSpecHook{
								{
									Unit: &models.Plugin{
										YamlMod: yamlPlugin,
									},
								},
							},
						},
					},
					Dependencies: map[string]models.JobSpecDependency{
						projectSpec.Name + "/" + inferredDependenciesPerJobID[jobSpec.ID][0].Name: {
							Project: &projectSpec,
							Job:     &inferredDependenciesPerJobID[jobSpec.ID][0],
						},
					},
					NamespaceSpec: namespaceSpec,
				},
			}

			actualJobSpecs, actualUnknownDependencies, actualError := dependencyResolver.GetJobSpecsWithDependencies(ctx, projectSpec.Name)

			assert.EqualValues(t, expectedJobSpecs, actualJobSpecs)
			assert.Empty(t, actualUnknownDependencies)
			assert.NoError(t, actualError)
		})
	})
}
