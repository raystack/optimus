package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReplayWorker(t *testing.T) {
	startDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-22")
	endDate, _ := time.Parse(job.ReplayDateFormat, "2020-08-26")
	currUUID := uuid.Must(uuid.NewRandom())
	replayRequest := models.ReplayRequestInput{
		ID: currUUID,
		Job: models.JobSpec{
			Name: "job-name",
		},
		Start: startDate,
		End:   endDate,
		Project: models.ProjectSpec{
			Name: "project-name",
		},
		DagSpecMap: make(map[string]models.JobSpec),
	}
	replaySpecToInsert := &models.ReplaySpec{
		ID:        currUUID,
		StartDate: startDate,
		EndDate:   endDate,
		Status:    models.ReplayStatusAccepted,
		Project:   replayRequest.Project,
		Job:       replayRequest.Job,
	}
	t.Run("Process", func(t *testing.T) {
		t.Run("should throw an error when replayRepo throws an error", func(t *testing.T) {
			ctx := context.Background()
			replayRepository := new(mock.ReplayRepository)
			defer replayRepository.AssertExpectations(t)
			errMessage := "replay repo error"

			replayRepository.On("Insert", replaySpecToInsert).Return(errors.New(errMessage))

			worker := job.NewReplayWorker(replayRepository, nil)
			err := worker.Process(ctx, replayRequest)
			assert.NotNil(t, err)
			assert.Equal(t, errMessage, err.Error())
		})
	})
}
