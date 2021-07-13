package mock

import (
	"context"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/mock"
)

type Scheduler struct {
	mock.Mock
}

func (ms *Scheduler) GetName() string {
	return ""
}

func (ms *Scheduler) GetTemplate() []byte {
	return []byte{}
}

func (ms *Scheduler) GetJobsDir() string {
	return ""
}

func (ms *Scheduler) GetJobsExtension() string {
	return ""
}

func (ms *Scheduler) Bootstrap(ctx context.Context, projectSpec models.ProjectSpec) error {
	return ms.Called(ctx, projectSpec).Error(0)
}

func (ms *Scheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string) ([]models.JobStatus, error) {
	args := ms.Called(ctx, projSpec, jobName)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

func (ms *Scheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	args := ms.Called(ctx, projSpec, jobName, startDate, endDate)
	return args.Error(0)
}
