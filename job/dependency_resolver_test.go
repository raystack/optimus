package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return(jobSpec2, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			// task dependencies
			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: jobSpec1Sources}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return(jobSpec2, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return(jobSpec2, errors.New("random error"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table2_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(
				&models.GenerateDependenciesResponse{Dependencies: jobSpec1Sources}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, nil)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table3_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			errorMsg := "internal error"
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table3_destination").Return(models.JobSpec{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			jobSpec1Sources := []string{"project.dataset.table3_destination"}
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, jobSpec1Sources[0]).Return(jobSpec1, nil)
			jobSpecRepository.On("GetByName", ctx, "static_dep").Return(models.JobSpec{}, models.NamespaceSpec{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, jobSpec1Sources[0]).Return(jobSpec1, nil)
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, jobSpec1Sources[0]).Return(jobSpec2, nil)
			jobSpecRepository.On("GetByName", ctx, "test3").Return(jobSpec3, namespaceSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpec1 := models.JobSource{
				JobID:       jobSpec1.ID,
				ProjectID:   projectSpec.ID,
				ResourceURN: jobSpec1Sources[0],
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpec1).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, jobSpec1Sources[0]).Return(jobSpec2, nil)
			jobSpecRepository.On("GetByDestination", ctx, jobSpec1Sources[1]).Return(jobSpecExternal, nil)
			jobSpecRepository.On("GetByNameForProject", ctx, externalProjectName, "test3").Return(jobSpec3, externalProjectSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: jobSpec1Sources,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceSpecs1 := []models.JobSource{
				{
					JobID:       jobSpec1.ID,
					ProjectID:   projectSpec.ID,
					ResourceURN: jobSpec1Sources[0],
				},
				{
					JobID:       jobSpec1.ID,
					ProjectID:   projectSpec.ID,
					ResourceURN: jobSpec1Sources[1],
				},
			}
			jobSourceRepo.On("Save", ctx, jobSourceSpecs1[0]).Return(nil)
			jobSourceRepo.On("Save", ctx, jobSourceSpecs1[1]).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, pluginService, jobSourceRepo)
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

	t.Run("ResolveInferredDependencies", func(t *testing.T) {
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
			dependenciesJob1 := []string{"project.dataset.table2_destination"}

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: dependenciesJob1}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(nil, pluginService, nil)
			resultJob1, err := resolver.ResolveInferredDependencies(ctx, projectSpec, jobSpec1)
			assert.Nil(t, err)
			resultJob2, err := resolver.ResolveInferredDependencies(ctx, projectSpec, jobSpec2)
			assert.Nil(t, err)

			assert.Equal(t, dependenciesJob1, resultJob1)
			assert.Nil(t, resultJob2)
		})
		t.Run("it should resolve runtime dependencies when more than 1 destination are found for a job", func(t *testing.T) {
			execUnit1 := new(mock.DependencyResolverMod)
			defer execUnit1.AssertExpectations(t)

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
			dependenciesJob1 := []string{
				"project.dataset.tablex_destination",
				"project.dataset.table2_destination",
			}

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{
				Dependencies: dependenciesJob1,
			}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec2, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(nil, pluginService, nil)
			resultJob1, err := resolver.ResolveInferredDependencies(ctx, projectSpec, jobSpec1)
			assert.Nil(t, err)
			resultJob2, err := resolver.ResolveInferredDependencies(ctx, projectSpec, jobSpec2)
			assert.Nil(t, err)

			assert.Equal(t, dependenciesJob1, resultJob1)
			assert.Nil(t, resultJob2)
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

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, jobSpec1, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, errors.New("random error"))
			defer pluginService.AssertExpectations(t)

			resolver := job.NewDependencyResolver(nil, pluginService, nil)
			resultJob1, err := resolver.ResolveInferredDependencies(ctx, projectSpec, jobSpec1)

			assert.Equal(t, "random error", err.Error())
			assert.Nil(t, resultJob1)
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

	t.Run("ResolveStaticDependencies", func(t *testing.T) {
		t.Run("should return nil and error if context is nil", func(t *testing.T) {
			projectJobFactory := &mock.ProjectJobSpecRepoFactory{}
			pluginService := &mock.DependencyResolverPluginService{}
			jobSourceRepo := &mock.JobSourceRepository{}
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
			dependencyResolver := job.NewDependencyResolver(projectJobFactory, pluginService, jobSourceRepo)

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
}
