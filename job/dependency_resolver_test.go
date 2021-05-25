package job_test

import (
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestDependencyResolver(t *testing.T) {
	t.Run("Resolve", func(t *testing.T) {
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
			execUnit1 := new(mock.Transformer)
			defer execUnit1.AssertExpectations(t)

			hookUnit1 := new(mock.HookUnit)
			defer hookUnit1.AssertExpectations(t)
			hookUnit2 := new(mock.HookUnit)
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
					Unit: execUnit1,
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
						Unit:      hookUnit1,
						DependsOn: nil,
					},
					{
						Config:    nil,
						Unit:      hookUnit2,
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
					Unit: execUnit1,
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
			jobSpecRepository.On("GetByDestination", "project.dataset.table2_destination").Return(jobSpec2, projectSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap(),
				Project: projectSpec,
			}

			// task dependencies
			execUnit1.On("GenerateDependencies", unitData).Return(models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)
			execUnit1.On("GenerateDependencies", unitData2).Return(models.GenerateDependenciesResponse{}, nil)

			// hook dependency
			hookUnit1.On("Name").Return("hook1")
			hookUnit1.On("DependsOn").Return([]string{})
			hookUnit2.On("DependsOn").Return([]string{"hook1"})

			resolver := job.NewDependencyResolver()
			resolvedJobSpec1, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
			assert.Equal(t, []*models.JobSpecHook{&resolvedJobSpec1.Hooks[0]}, resolvedJobSpec1.Hooks[1].DependsOn)
		})
		t.Run("it should resolve all dependencies including static unresolved dependency", func(t *testing.T) {
			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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
					Unit: execUnit,
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
					Unit: execUnit,
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
			jobSpecRepository.On("GetByDestination", "project.dataset.table2_destination").Return(jobSpec2, projectSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap(),
				Project: projectSpec,
			}

			execUnit.On("GenerateDependencies", unitData).Return(models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"},
			}, nil)
			execUnit.On("GenerateDependencies", unitData2).Return(models.GenerateDependenciesResponse{}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpec1, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
				jobSpec3.Name: {Job: &jobSpec3, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
		})

		t.Run("should fail if GetByDestination fails", func(t *testing.T) {
			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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
					Unit: execUnit,
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
			jobSpecRepository.On("GetByDestination", "project.dataset.table2_destination").Return(jobSpec2, projectSpec, errors.New("random error"))
			defer jobSpecRepository.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(), Project: projectSpec}
			execUnit.On("GenerateDependencies", unitData).Return(
				models.GenerateDependenciesResponse{Dependencies: []string{"project.dataset.table2_destination"}}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpec1, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)

			assert.Error(t, errors.Wrapf(errors.New("random error"), job.UnknownRuntimeDependencyMessage,
				"project.dataset.table2_destination", jobSpec1.Name),
				err.Error())
			assert.Equal(t, models.JobSpec{}, resolvedJobSpec1)
		})

		t.Run("should fail if GenerateDependencies fails", func(t *testing.T) {
			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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

			unitData := models.GenerateDependenciesRequest{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(), Project: projectSpec}
			execUnit.On("GenerateDependencies", unitData).Return(models.GenerateDependenciesResponse{}, errors.New("random error"))

			resolver := job.NewDependencyResolver()
			resolvedJobSpec1, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)

			assert.Equal(t, "random error", err.Error())
			assert.Equal(t, models.JobSpec{}, resolvedJobSpec1)
		})

		t.Run("should fail if job destination is undefined", func(t *testing.T) {
			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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
			jobSpecRepository.On("GetByDestination", "project.dataset.table3_destination").Return(nil, nil, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(), Project: projectSpec}
			execUnit.On("GenerateDependencies", unitData).Return(models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table3_destination"}}, nil)

			resolver := job.NewDependencyResolver()
			_, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)
			assert.Error(t, errors.Wrapf(errors.New("spec not found"), job.UnknownRuntimeDependencyMessage,
				"project.dataset.table3_destination", jobSpec1.Name),
				err.Error())
		})

		t.Run("it should fail for unknown static dependency", func(t *testing.T) {
			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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
					Unit: execUnit,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{"static_dep": {Job: nil}},
			}

			jobSpecRepository := new(mock.ProjectJobSpecRepository)
			jobSpecRepository.On("GetByDestination", "project.dataset.table1_destination").Return(jobSpec1, projectSpec, nil)
			jobSpecRepository.On("GetByName", "static_dep").Return(nil, errors.New("spec not found"))
			defer jobSpecRepository.AssertExpectations(t)

			unitData2 := models.GenerateDependenciesRequest{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap(), Project: projectSpec}
			execUnit.On("GenerateDependencies", unitData2).Return(models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table1_destination"},
			}, nil)

			resolver := job.NewDependencyResolver()
			_, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec2, nil)
			assert.Equal(t, "unknown local dependency for job static_dep: spec not found", err.Error())
		})

		t.Run("it should resolve any unresolved static dependency", func(t *testing.T) {
			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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
					Unit: execUnit,
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
					Unit: execUnit,
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
			jobSpecRepository.On("GetByDestination", "project.dataset.table2_destination").Return(jobSpec2, projectSpec, nil)
			jobSpecRepository.On("GetByName", "test3").Return(jobSpec3, namespaceSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap(),
				Project: projectSpec,
			}

			execUnit.On("GenerateDependencies", unitData).Return(models.GenerateDependenciesResponse{
				Dependencies: []string{"project.dataset.table2_destination"},
			}, nil)
			execUnit.On("GenerateDependencies", unitData2).Return(models.GenerateDependenciesResponse{}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpec1, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Nil(t, err)
			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
				jobSpec3.Name: {Job: &jobSpec3, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra},
			}, resolvedJobSpec1.Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
		})

		t.Run("it should resolve any inter dependency", func(t *testing.T) {
			externalProjectName := "an-external-data-project"
			externalProjectSpec := models.ProjectSpec{
				Name: externalProjectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			execUnit := new(mock.Transformer)
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
					Unit: execUnit,
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
					Unit: execUnit,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "bar",
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: nil, Type: models.JobSpecDependencyTypeIntra}},
				// explicitly setting a dirty dependency
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
					Unit: execUnit,
					Config: models.JobSpecConfigs{
						{
							Name:  "foo",
							Value: "baz",
						},
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecExternal := models.JobSpec{
				Version: 1,
				Name:    "test2-external",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: execUnit,
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
			jobSpecRepository.On("GetByDestination", "project.dataset.table2_destination").Return(jobSpec2, projectSpec, nil)
			jobSpecRepository.On("GetByDestination", "project.dataset.table2_external_destination").Return(jobSpecExternal, externalProjectSpec, nil)
			jobSpecRepository.On("GetByName", "test3").Return(jobSpec3, namespaceSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			unitData := models.GenerateDependenciesRequest{
				Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap(),
				Project: projectSpec,
			}
			unitData2 := models.GenerateDependenciesRequest{
				Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap(),
				Project: projectSpec,
			}

			execUnit.On("GenerateDependencies", unitData).Return(models.GenerateDependenciesResponse{
				Dependencies: []string{
					"project.dataset.table2_destination",
					"project.dataset.table2_external_destination", // inter optimus depedency
				},
			}, nil)
			execUnit.On("GenerateDependencies", unitData2).Return(models.GenerateDependenciesResponse{}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpec1, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec1, nil)
			assert.Nil(t, err)
			resolvedJobSpec2, err := resolver.Resolve(projectSpec, jobSpecRepository, jobSpec2, nil)
			assert.Nil(t, err)

			assert.Nil(t, err)
			assert.Equal(t, models.JobSpecDependency{Job: &jobSpec2, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra}, resolvedJobSpec1.Dependencies[jobSpec2.Name])
			assert.Equal(t, models.JobSpecDependency{Job: &jobSpecExternal, Project: &externalProjectSpec, Type: models.JobSpecDependencyTypeInter}, resolvedJobSpec1.Dependencies[jobSpecExternal.Name])
			assert.Equal(t, models.JobSpecDependency{Job: &jobSpec3, Project: &projectSpec, Type: models.JobSpecDependencyTypeIntra}, resolvedJobSpec1.Dependencies[jobSpec3.Name])
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpec2.Dependencies)
		})
	})
}
