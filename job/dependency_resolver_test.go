package job_test

import (
	"sort"
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
		t.Run("it should resolve runtime dependencies", func(t *testing.T) {
			execUnit1 := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "bar",
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			unitData2 := models.UnitData{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap()}

			// task dependencies
			execUnit1.On("GenerateDestination", unitData).Return("project.dataset.table1_destination", nil)
			execUnit1.On("GenerateDestination", unitData2).Return("project.dataset.table2_destination", nil)
			execUnit1.On("GenerateDependencies", unitData).Return([]string{"project.dataset.table2_destination"}, nil)
			execUnit1.On("GenerateDependencies", unitData2).Return([]string{}, nil)

			// hook dependency
			hookUnit1.On("GetName").Return("hook1")
			hookUnit1.On("GetDependsOn").Return([]string{})
			hookUnit2.On("GetDependsOn").Return([]string{"hook1"})

			resolver := job.NewDependencyResolver()
			resolvedJobSpecs, err := resolver.Resolve(jobSpecs)
			sort.Slice(resolvedJobSpecs, func(i, j int) bool { return resolvedJobSpecs[i].Name < resolvedJobSpecs[j].Name })

			assert.Nil(t, err)
			assert.Equal(t, map[string]models.JobSpecDependency{jobSpec2.Name: {Job: &jobSpec2}}, resolvedJobSpecs[0].Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpecs[1].Dependencies)
			assert.Equal(t, []*models.JobSpecHook{&resolvedJobSpecs[0].Hooks[0]},
				resolvedJobSpecs[0].Hooks[1].DependsOn)
		})

		t.Run("it should resolve all dependencies including static unresolved dependency", func(t *testing.T) {
			execUnit := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "baa",
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
					Config: map[string]string{
						"foo": "bar",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: &jobSpec3}},
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2, jobSpec3}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			unitData2 := models.UnitData{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap()}
			unitData3 := models.UnitData{Config: jobSpec3.Task.Config, Assets: jobSpec3.Assets.ToMap()}

			execUnit.On("GenerateDestination", unitData).Return("project.dataset.table1_destination", nil)
			execUnit.On("GenerateDestination", unitData2).Return("project.dataset.table2_destination", nil)
			execUnit.On("GenerateDestination", unitData3).Return("project.dataset.table3_destination", nil)
			execUnit.On("GenerateDependencies", unitData).Return([]string{"project.dataset.table2_destination"}, nil)
			execUnit.On("GenerateDependencies", unitData2).Return([]string{}, nil)
			execUnit.On("GenerateDependencies", unitData3).Return([]string{}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpecs, err := resolver.Resolve(jobSpecs)
			sort.Slice(resolvedJobSpecs, func(i, j int) bool { return resolvedJobSpecs[i].Name < resolvedJobSpecs[j].Name })

			assert.Nil(t, err)
			assert.Equal(t, map[string]models.JobSpecDependency{jobSpec2.Name: {Job: &jobSpec2}, jobSpec3.Name: {Job: &jobSpec3}}, resolvedJobSpecs[0].Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpecs[1].Dependencies)
		})

		t.Run("should fail if GenerateDestination fails", func(t *testing.T) {
			execUnit := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "bar",
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			execUnit.On("GenerateDestination", unitData).Return("p.d.t", errors.New("random error"))

			resolver := job.NewDependencyResolver()
			resolvedJobSpecs, err := resolver.Resolve(jobSpecs)
			sort.Slice(resolvedJobSpecs, func(i, j int) bool { return resolvedJobSpecs[i].Name < resolvedJobSpecs[j].Name })

			assert.Equal(t, "random error", err.Error())
			assert.Nil(t, resolvedJobSpecs)
		})

		t.Run("should fail if GenerateDependencies fails", func(t *testing.T) {
			execUnit := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "bar",
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			unitData2 := models.UnitData{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap()}
			execUnit.On("GenerateDestination", unitData).Return("p.d.t", nil)
			execUnit.On("GenerateDestination", unitData2).Return("p.d.t", nil)
			execUnit.On("GenerateDependencies", unitData).Return([]string{"p.d.t"}, errors.New("random error"))

			resolver := job.NewDependencyResolver()
			resolvedJobSpecs, err := resolver.Resolve(jobSpecs)
			sort.Slice(resolvedJobSpecs, func(i, j int) bool { return resolvedJobSpecs[i].Name < resolvedJobSpecs[j].Name })

			assert.Equal(t, "failed to resolve dependency destination for test1: random error", err.Error())
			assert.Nil(t, resolvedJobSpecs)
		})

		t.Run("should fail if job destination is undefined", func(t *testing.T) {
			execUnit := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "bar",
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			unitData2 := models.UnitData{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap()}
			execUnit.On("GenerateDestination", unitData).Return("project.dataset.table1_destination", nil)
			execUnit.On("GenerateDestination", unitData2).Return("project.dataset.table2_destination", nil)
			execUnit.On("GenerateDependencies", unitData).Return([]string{"project.dataset.table3_destination"}, nil)

			resolver := job.NewDependencyResolver()
			_, err := resolver.Resolve(jobSpecs)
			assert.Equal(t, "invalid job specs, undefined destination project.dataset.table3_destination", err.Error())
		})

		t.Run("it should fail for unknown static dependency", func(t *testing.T) {
			execUnit := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "bar",
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{"static_dep": {Job: nil}},
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			unitData2 := models.UnitData{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap()}

			execUnit.On("GenerateDestination", unitData).Return("project.dataset.table1_destination", nil)
			execUnit.On("GenerateDestination", unitData2).Return("project.dataset.table2_destination", nil)
			execUnit.On("GenerateDependencies", unitData).Return([]string{"project.dataset.table2_destination"}, nil)
			execUnit.On("GenerateDependencies", unitData2).Return([]string{"project.dataset.table1_destination"}, nil)

			resolver := job.NewDependencyResolver()
			_, err := resolver.Resolve(jobSpecs)
			assert.Equal(t, "static_dep: unknown dependency", err.Error())
		})

		t.Run("it should resolve any unresolved static dependency", func(t *testing.T) {
			execUnit := new(mock.ExecutionUnit)
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
					Config: map[string]string{
						"foo": "baa",
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
					Config: map[string]string{
						"foo": "bar",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{"test3": {Job: nil}}, // explicitly setting this to nil. which should get resolved
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
					Config: map[string]string{
						"foo": "baz",
					},
				},
				Dependencies: make(map[string]models.JobSpecDependency),
			}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2, jobSpec3}

			unitData := models.UnitData{Config: jobSpec1.Task.Config, Assets: jobSpec1.Assets.ToMap()}
			unitData2 := models.UnitData{Config: jobSpec2.Task.Config, Assets: jobSpec2.Assets.ToMap()}
			unitData3 := models.UnitData{Config: jobSpec3.Task.Config, Assets: jobSpec3.Assets.ToMap()}

			execUnit.On("GenerateDestination", unitData).Return("project.dataset.table1_destination", nil)
			execUnit.On("GenerateDestination", unitData2).Return("project.dataset.table2_destination", nil)
			execUnit.On("GenerateDestination", unitData3).Return("project.dataset.table3_destination", nil)
			execUnit.On("GenerateDependencies", unitData).Return([]string{"project.dataset.table2_destination"}, nil)
			execUnit.On("GenerateDependencies", unitData2).Return([]string{}, nil)
			execUnit.On("GenerateDependencies", unitData3).Return([]string{}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpecs, err := resolver.Resolve(jobSpecs)
			sort.Slice(resolvedJobSpecs, func(i, j int) bool { return resolvedJobSpecs[i].Name < resolvedJobSpecs[j].Name })

			assert.Nil(t, err)
			assert.Equal(t, map[string]models.JobSpecDependency{jobSpec2.Name: {Job: &jobSpec2}, jobSpec3.Name: {Job: &jobSpec3}}, resolvedJobSpecs[0].Dependencies)
			assert.Equal(t, map[string]models.JobSpecDependency{}, resolvedJobSpecs[1].Dependencies)
		})
	})
}
