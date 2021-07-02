package models

import (
	"context"
	"time"
)

var (
	// Scheduler is a single unit initialized at the start of application
	// based on config. This will be used to perform adhoc operations
	// to support target scheduling engine
	Scheduler SchedulerUnit

	JobStatusStateSuccess JobStatusState = "success"
	JobStatusStateFailed  JobStatusState = "failed"
	JobStatusStateRunning JobStatusState = "running"
)

// SchedulerUnit is implemented by supported schedulers
type SchedulerUnit interface {
	GetName() string

	// GetTemplate returns template file used during spec compilation.
	// Each scheduler needs to have a template file using which optimus
	// compiler will inject its spec details
	GetTemplate() []byte

	// GetJobsDir returns the directory to use while storing compiled
	// scheduler specific input files
	GetJobsDir() string

	// GetJobsExtension provides extension to use for input files of scheduler
	GetJobsExtension() string

	// Bootstrap will be executed per project when the application boots up
	// this can be used to do adhoc commands for initialization of scheduler
	Bootstrap(context.Context, ProjectSpec) error

	// GetJobStatus should return the current and previous status of job
	GetJobStatus(ctx context.Context, projSpec ProjectSpec, jobName string) ([]JobStatus, error)

	// Clear clears state of job between provided start and end dates
	Clear(ctx context.Context, projSpec ProjectSpec, jobName string, startDate, endDate time.Time) error

	// GetDagRunStatus should return batch of runs of a job
	GetDagRunStatus(ctx context.Context, projSpec ProjectSpec, jobName string, startDate time.Time, endDate time.Time,
		batchSize int) ([]JobStatus, error)
}

type JobStatusState string

func (j JobStatusState) String() string {
	return string(j)
}

type JobStatus struct {
	ScheduledAt time.Time
	State       JobStatusState
}
