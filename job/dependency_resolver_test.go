package job_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestResolver(t *testing.T) {
	t.Run("Resolve", func(t *testing.T) {
		t.Run("it should resolve dependencies", func(t *testing.T) {
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
			execUnit.On("GenerateDependencies", unitData).Return([]string{"project.dataset.table2_destination"}, nil)
			execUnit.On("GenerateDestination", unitData2).Return("project.dataset.table2_destination", nil)
			execUnit.On("GenerateDependencies", unitData2).Return([]string{}, nil)

			resolver := job.NewDependencyResolver()
			resolvedJobSpecs, err := resolver.Resolve(jobSpecs)
			sort.Slice(resolvedJobSpecs, func(i, j int) bool { return resolvedJobSpecs[i].Name < resolvedJobSpecs[j].Name })

			assert.Nil(t, err)
			fmt.Println(resolvedJobSpecs)
			assert.Equal(t, map[string]models.JobSpecDependency{
				jobSpec2.Name: {Job: &jobSpec2},
			}, resolvedJobSpecs[0].Dependencies)
		})
	})
}
