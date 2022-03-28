package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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
				},
			}, nil)
			execUnit1.On("GenerateDependencies", ctx, unitData2).Return(&models.GenerateDependenciesResponse{}, nil)

			// hook dependency
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "hook1",
			}, nil)
			hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:      "hook2",
				DependsOn: []string{"hook1"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: &jobSpec3, Type: models.JobSpecDependencyTypeIntra}},
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)
			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets), Project: projectSpec}
			execUnit.On("GenerateDependencies", context.Background(), unitData).Return(&models.GenerateDependenciesResponse{}, errors.New("random error"))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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
				Dependencies: []string{"project.dataset.table3_destination"},
			}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: nil, Type: models.JobSpecDependencyTypeIntra}},
				// explicitly setting this to nil. which should get resolved
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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
				// explicitly setting a dirty intra dependency
				Dependencies: map[string]models.JobSpecDependency{externalProjectName + "/" + jobSpec3.Name: {Job: nil, Type: models.JobSpecDependencyTypeInter}},
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
				Dependencies: make(map[string]models.JobSpecDependency),
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

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, nil)
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

	t.Run("ResolveAndPersist", func(t *testing.T) {
		ctx := context.Background()
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			Name: projectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}

		t.Run("should able to resolve and persist successfully", func(t *testing.T) {
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)
			jobDependencyRepository.On("DeleteByJobID", ctx, jobSpec1.ID).Return(nil)
			jobSpecDependency := models.JobSpecDependency{
				Project: &projectSpec,
				Job:     &jobSpec2,
				Type:    models.JobSpecDependencyTypeIntra,
			}
			jobDependencyRepository.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpecDependency).Return(nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			err := resolver.ResolveAndPersist(ctx, projectSpec, jobSpec1, nil)

			assert.Nil(t, err)
		})
		t.Run("should return fail when unable to generate dependencies", func(t *testing.T) {
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
			}
			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			errorMsg := "generate dependencies failed"
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{}, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			err := resolver.ResolveAndPersist(ctx, projectSpec, jobSpec1, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should return fail when unable to get by destination", func(t *testing.T) {
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
			}
			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			errorMsg := "internal error"
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return(nil, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			err := resolver.ResolveAndPersist(ctx, projectSpec, jobSpec1, nil)

			assert.Equal(t, fmt.Sprintf("runtime dependency evaluation failed: %s", errorMsg), err.Error())
		})
		t.Run("should return fail when failed to delete dependencies", func(t *testing.T) {
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)
			errorMsg := "internal error"
			jobDependencyRepository.On("DeleteByJobID", ctx, jobSpec1.ID).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			err := resolver.ResolveAndPersist(ctx, projectSpec, jobSpec1, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should return fail when failed to save dependency", func(t *testing.T) {
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			unitData := models.GenerateDependenciesRequest{
				Config: models.PluginConfigs{}.FromJobSpec(jobSpec1.Task.Config), Assets: models.PluginAssets{}.FromJobSpec(jobSpec1.Assets),
				Project: projectSpec,
			}

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			execUnit1.On("GenerateDependencies", ctx, unitData).Return(&models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			jobSpecRepository.On("GetByDestination", ctx, "project.dataset.table2_destination").Return([]store.ProjectJobPair{{
				Project: projectSpec, Job: jobSpec2,
			}}, nil)

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)
			jobDependencyRepository.On("DeleteByJobID", ctx, jobSpec1.ID).Return(nil)
			errorMsg := "internal error"
			jobSpecDependency := models.JobSpecDependency{
				Project: &projectSpec,
				Job:     &jobSpec2,
				Type:    models.JobSpecDependencyTypeIntra,
			}
			jobDependencyRepository.On("Save", ctx, projectSpec.ID, jobSpec1.ID, jobSpecDependency).Return(errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			err := resolver.ResolveAndPersist(ctx, projectSpec, jobSpec1, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
	})

	t.Run("FetchJobDependencies", func(t *testing.T) {
		ctx := context.Background()
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}

		t.Run("should able to fetch dependencies", func(t *testing.T) {
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			expectedDependency := map[string]models.JobSpecDependency{
				jobSpec2.Name: {
					Project: &projectSpec,
					Job:     &jobSpec2,
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpec1.Dependencies = expectedDependency
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			projectJobSpecRepository.On("GetAll", ctx).Return(jobSpecs, nil)

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)
			persistedDependencies := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec2.ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
			}
			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return(persistedDependencies, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			actual, err := resolver.FetchJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, err)
			assert.EqualValues(t, expectedDependency[jobSpec2.Name], actual[jobSpec1.ID][0])
		})
		t.Run("should able to fetch dependencies including inter dependencies", func(t *testing.T) {
			execUnit1 := new(mock.DependencyResolverMod)
			defer execUnit1.AssertExpectations(t)
			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

			jobSpec1 := models.JobSpec{
				ID:      uuid.Must(uuid.NewRandom()),
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
			}
			otherProjectSpec := models.ProjectSpec{
				Name: "b-data-project",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}
			jobSpec2 := models.JobSpec{
				Version: 1,
				ID:      uuid.Must(uuid.NewRandom()),
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec3 := models.JobSpec{
				Version: 1,
				ID:      uuid.Must(uuid.NewRandom()),
				Name:    "test3",
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1}
			dependencies := []models.JobSpecDependency{
				{
					Project: &otherProjectSpec,
					Job:     &jobSpec2,
					Type:    models.JobSpecDependencyTypeInter,
				},
				{
					Project: &otherProjectSpec,
					Job:     &jobSpec3,
					Type:    models.JobSpecDependencyTypeInter,
				},
			}
			jobSpec1.Dependencies = map[string]models.JobSpecDependency{
				jobSpec2.Name: dependencies[0],
				jobSpec3.Name: dependencies[1],
			}

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			persistedDependencies := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec2.ID,
					DependentProject: otherProjectSpec,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec3.ID,
					DependentProject: otherProjectSpec,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)
			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return(persistedDependencies, nil)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			projectJobSpecRepository.On("GetAll", ctx).Return(jobSpecs, nil)

			projectJobSpecRepoFactory.On("New", otherProjectSpec).Return(jobSpecRepository)
			jobSpecRepository.On("GetByIDs", ctx, []uuid.UUID{jobSpec2.ID, jobSpec3.ID}).Return([]models.JobSpec{jobSpec2, jobSpec3}, nil)

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			actual, err := resolver.FetchJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, err)
			assert.EqualValues(t, dependencies, []models.JobSpecDependency{actual[jobSpec1.ID][0], actual[jobSpec1.ID][1]})
		})
		t.Run("should failed when unable to get persisted dependencies", func(t *testing.T) {
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			expectedDependency := map[string]models.JobSpecDependency{
				jobSpec2.Name: {
					Project: &projectSpec,
					Job:     &jobSpec2,
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpec1.Dependencies = expectedDependency

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)

			errorMsg := "internal error"
			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return([]models.JobIDDependenciesPair{}, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			actual, err := resolver.FetchJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, actual)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should failed when unable to get inter dependent jobspec", func(t *testing.T) {
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
			}
			otherProjectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "b-data-project",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpec3 := models.JobSpec{
				Version: 1,
				Name:    "test3",
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
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1}
			expectedDependency := map[string]models.JobSpecDependency{
				jobSpec2.Name: {
					Project: &otherProjectSpec,
					Job:     &jobSpec2,
					Type:    models.JobSpecDependencyTypeInter,
				},
				jobSpec3.Name: {
					Project: &otherProjectSpec,
					Job:     &jobSpec3,
					Type:    models.JobSpecDependencyTypeInter,
				},
			}
			jobSpec1.Dependencies = expectedDependency

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobDependencyRepoFactory := new(mock.JobDependencyRepoFactory)
			defer jobDependencyRepoFactory.AssertExpectations(t)

			jobDependencyRepository := new(mock.JobDependencyRepository)
			defer jobDependencyRepository.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobDependencyRepoFactory.On("New", projectSpec).Return(jobDependencyRepository)
			jobDependencies := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec2.ID,
					DependentProject: otherProjectSpec,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobSpec1.ID,
					DependentJobID:   jobSpec3.ID,
					DependentProject: otherProjectSpec,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}
			jobDependencyRepository.On("GetAll", ctx, projectSpec.ID).Return(jobDependencies, nil)

			projectJobSpecRepoFactory.On("New", projectSpec).Return(projectJobSpecRepository)
			projectJobSpecRepository.On("GetAll", ctx).Return(jobSpecs, nil)

			projectJobSpecRepoFactory.On("New", otherProjectSpec).Return(jobSpecRepository)
			errorMsg := "internal error"
			jobSpecRepository.On("GetByIDs", ctx, []uuid.UUID{jobSpec2.ID, jobSpec3.ID}).Return([]models.JobSpec{}, errors.New(errorMsg))

			resolver := job.NewDependencyResolver(projectJobSpecRepoFactory, jobDependencyRepoFactory)
			actual, err := resolver.FetchJobDependencies(ctx, projectSpec, nil)

			assert.Nil(t, actual)
			assert.Equal(t, errorMsg, err.Error())
		})
	})
}
