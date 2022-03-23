package prime

import (
	"context"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
)

// RunRepoFactory manages execution instances of a job runs
type RunRepoFactory interface {
	New() store.JobRunRepository
}

type InstanceRepoFactory interface {
	New() store.InstanceRepository
}

type Scheduler struct {
	jobRunRepoFac RunRepoFactory
	Now           func() time.Time
}

func (s *Scheduler) GetName() string {
	return "sequential"
}

func (s *Scheduler) VerifyJob(context.Context, models.NamespaceSpec, models.JobSpec) error {
	return nil
}

func (s *Scheduler) ListJobs(context.Context, models.NamespaceSpec, models.SchedulerListOptions) ([]models.Job, error) {
	panic("implement me")
}

func (s *Scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec, _ progress.Observer) error {
	var jobRuns []models.JobRun
	for _, j := range jobs {
		jobRuns = append(jobRuns, models.JobRun{
			Spec:        j,
			Trigger:     models.TriggerManual,
			ScheduledAt: s.Now(),
		})
	}

	repo := s.jobRunRepoFac.New()
	for _, runs := range jobRuns {
		if err := repo.Save(ctx, namespace, runs); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) DeleteJobs(context.Context, models.NamespaceSpec, []string, progress.Observer) error {
	return nil
}

func (s *Scheduler) Bootstrap(context.Context, models.ProjectSpec) error {
	return nil
}

func (s *Scheduler) GetJobStatus(context.Context, models.ProjectSpec, string) ([]models.JobStatus, error) {
	panic("implement me")
}

func (s *Scheduler) Clear(context.Context, models.ProjectSpec, string, time.Time, time.Time) error {
	return nil
}

func (s *Scheduler) GetJobRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate, endDate time.Time, batchSize int) ([]models.JobStatus, error) {
	panic("implement me")
}

func NewScheduler(jobRunRepoFac RunRepoFactory, nowFn func() time.Time) *Scheduler {
	return &Scheduler{
		jobRunRepoFac: jobRunRepoFac,
		Now:           nowFn,
	}
}
