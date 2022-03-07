package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets),
				Project: projectSpec,
			}

			// task dependencies
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			execUnit1.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
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

			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets),
				Project: projectSpec,
			}

			// task dependencies
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{
					"project.dataset.tablex_destination",
					"project.dataset.table2_destination",
				}}, nil)
			execUnit1.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
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

			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets),
				Project: projectSpec,
			}

			// task dependencies
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			execUnit1.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: &jobSpec3, Type: models.JobSpecDependencyTypeIntra}},
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets),
				Project: projectSpec,
			}

			execUnit.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"},
			}, nil)
			execUnit.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			}}, errors.New("random error"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets), Project: projectSpec}
			execUnit.On("GenerateDependencies", context.Background(), unitData).Return(
				&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets), Project: projectSpec}
			execUnit.On("GenerateDependencies", context.Background(), unitData).Return(&models.GenerateDependenciesResponse{}, errors.New("random error"))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table3_destination").Return([]store.ProjectJobPair{}, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets), Project: projectSpec}
			execUnit.On("GenerateDependencies", context.Background(), unitData).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"}}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: map[string]models.JobSpecDependency{"static_dep": {Job: nil, Type: models.JobSpecDependencyTypeIntra}},
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

			unitData2 := models.GenerateDependenciesRequest{Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets), Project: projectSpec}
			execUnit.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table1_destination"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: map[string]models.JobSpecDependency{"static_dep": {Job: nil, Type: "bad"}},
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table1_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec1,
			}}, nil)
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData2 := models.GenerateDependenciesRequest{Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets), Project: projectSpec}
			execUnit.On("GenerateDependencies", context.Background(), unitData2).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table1_destination"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
			jobSpecRepository.On("GetByName", ctx, "test3").Return(jobSpec3, namespaceSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets),
				Project: projectSpec,
			}

			execUnit.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"},
			}, nil)
			execUnit.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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
				Dependencies: map[string]models.JobSpecDependency{externalProjectName + "/" + jobSpec3.Name: {Job: nil, Type: models.JobSpecDependencyTypeInter}},
			}

			// destination: project.dataset.table2_destination
			jobSpec2 := models.JobSpec{
				Version: 1,
				Name:    "test2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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

			// destination: project.dataset.table2_external_destination
			jobSpecExternal := models.JobSpec{
				Version: 1,
				Name:    "test2-external",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
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

			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec2.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec2.Assets),
				Project: projectSpec,
			}

			execUnit.On("GenerateDependencies", context.Background(), unitData).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{
					"project.dataset.table2_destination",
					"project.dataset.table2_external_destination", // inter optimus dependency
				},
			}, nil)
			execUnit.On("GenerateDependencies", context.Background(), unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory)
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
}
