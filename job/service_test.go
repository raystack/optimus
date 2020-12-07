package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/core/mock"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
)

func TestDAGService(t *testing.T) {
	t.Run("CreateDAG", func(t *testing.T) {
		t.Run("it should create a new DAGSpec", func(t *testing.T) {
			jobInput := models.JobInput{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobInputSchedule{
					StartDate: "2020-12-02",
					Interval:  "@daily",
				},
			}

			repo := new(mock.JobSpecRepository)
			repo.On("Save", jobInput).Return(nil)
			defer repo.AssertExpectations(t)

			svc := job.NewService(repo)
			err := svc.CreateJob(jobInput)
			assert.Nil(t, err)
		})
	})
}
