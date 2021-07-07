package mock

import (
	"context"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/mock"
)

type MockScheduler struct {
	mock.Mock
}

func (ms *MockScheduler) GetName() string {
	return ""
}

func (ms *MockScheduler) GetTemplate() []byte {
	return []byte{}
}

func (ms *MockScheduler) GetJobsDir() string {
	return ""
}

func (ms *MockScheduler) GetJobsExtension() string {
	return ""
}

func (ms *MockScheduler) Bootstrap(ctx context.Context, projectSpec models.ProjectSpec) error {
	return ms.Called(ctx, projectSpec).Error(0)
}

func (ms *MockScheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string) ([]models.JobStatus, error) {
	args := ms.Called(ctx, projSpec, jobName)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}

func (ms *MockScheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	args := ms.Called(ctx, projSpec, jobName, startDate, endDate)
	return args.Error(0)
}

func (ms *MockScheduler) GetDagRunStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate time.Time,
	endDate time.Time, batchSize int) ([]models.JobStatus, error) {
	args := ms.Called(ctx, projSpec, jobName, startDate, endDate, batchSize)
	return args.Get(0).([]models.JobStatus), args.Error(1)
}
