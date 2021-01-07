package job_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestDAGService(t *testing.T) {
	t.Run("CreateDAG", func(t *testing.T) {
		t.Run("it should create a new DAGSpec", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 02, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
			}
			projSpec := models.ProjectSpec{
				Name: "proj",
			}

			repo := new(mock.JobSpecRepository)
			repo.On("Save", jobSpec).Return(nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", projSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, nil)
			err := svc.CreateJob(jobSpec, projSpec)
			assert.Nil(t, err)
		})
	})
}
