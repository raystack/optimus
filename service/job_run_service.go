package service

import (
	"context"

	"github.com/odpf/optimus/models"
)

type JobRunService interface {
	// GetJobRunList returns all the job based given status and date range
	GetJobRunList(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec, jobQuery *models.JobQuery) ([]models.JobRun, error)
}

type jobRunService struct {
	scheduler models.SchedulerUnit
}

func NewJobRunService(scheduler models.SchedulerUnit) *jobRunService {
	return &jobRunService{
		scheduler: scheduler,
	}
}
