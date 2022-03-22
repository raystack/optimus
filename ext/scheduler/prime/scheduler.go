package prime

import (
	"context"
	"time"

	"github.com/odpf/optimus/core/cron"

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

func (s *Scheduler) VerifyJob(ctx context.Context, namespace models.NamespaceSpec, job models.JobSpec) error {
	return nil
}

func (s *Scheduler) ListJobs(ctx context.Context, namespace models.NamespaceSpec, opts models.SchedulerListOptions) ([]models.Job, error) {
	panic("implement me")
}

func (s *Scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec, obs progress.Observer) error {
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

func (s *Scheduler) DeleteJobs(ctx context.Context, namespace models.NamespaceSpec, jobNames []string, obs progress.Observer) error {
	return nil
}

func (s *Scheduler) Bootstrap(ctx context.Context, spec models.ProjectSpec) error {
	return nil
}

func (s *Scheduler) GetJobStatus(ctx context.Context, projSpec models.ProjectSpec, jobName string) ([]models.JobStatus, error) {
	panic("implement me")
}

func (s *Scheduler) Clear(ctx context.Context, projSpec models.ProjectSpec, jobName string, startDate, endDate time.Time) error {
	return nil
}

func (s *Scheduler) GetJobRunStatus(ctx context.Context, projectSpec models.ProjectSpec, jobName string, startDate time.Time, endDate time.Time, batchSize int) ([]models.JobStatus, error) {
	panic("implement me")
}

func (s *Scheduler) GetJobRuns(ctx context.Context, projectSpec models.ProjectSpec, param *models.JobQuery, spec *cron.ScheduleSpec) ([]models.JobRun, error) {
	return []models.JobRun{}, nil
}

func NewScheduler(jobRunRepoFac RunRepoFactory, nowFn func() time.Time) *Scheduler {
	return &Scheduler{
		jobRunRepoFac: jobRunRepoFac,
		Now:           nowFn,
	}
}
