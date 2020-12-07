package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
)

func TestFactory(t *testing.T) {
	t.Run("CreateJob", func(t *testing.T) {
		factory := job.Factory{}
		t.Run("catchup default value should be true", func(t *testing.T) {
			spec, _ := factory.CreateJobSpec(models.JobInput{
				Version: 1,
				Name:    "asdasd",
				Schedule: models.JobInputSchedule{
					StartDate: "2000-11-11",
					Interval:  "@daily",
				},
			})
			assert.True(t, spec.Behavior.Catchup)
		})
		t.Run("depends_on_past default value should be false", func(t *testing.T) {
			spec, _ := factory.CreateJobSpec(models.JobInput{
				Version: 1,
				Name:    "asdasd",
				Schedule: models.JobInputSchedule{
					StartDate: "2000-11-11",
					Interval:  "@daily",
				},
			})
			assert.False(t, spec.Behavior.DependsOnPast)
		})
	})
}
