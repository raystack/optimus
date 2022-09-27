package prime

import (
	"context"
	"time"

	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Scheduler struct {
	jobRunRepo store.JobRunRepository
	Now        func() time.Time
}

func (*Scheduler) GetName() string {
	return "sequential"
}

func (*Scheduler) VerifyJob(context.Context, models.NamespaceSpec, models.JobSpec) error {
	return nil
}

func (*Scheduler) ListJobs(context.Context, string, models.NamespaceSpec, models.SchedulerListOptions) ([]models.Job, error) {
	panic("implement me")
}

func (s *Scheduler) DeployJobs(ctx context.Context, namespace models.NamespaceSpec, jobs []models.JobSpec) (models.JobDeploymentDetail, error) {
	var jobRuns []models.JobRun
	for _, j := range jobs {
		jobRuns = append(jobRuns, models.JobRun{
			Spec:        j,
			Trigger:     models.TriggerManual,
			ScheduledAt: s.Now(),
		})
	}

	jobDestination := "" // fetch job destination from plugin service
	for _, runs := range jobRuns {
		if err := s.jobRunRepo.Save(ctx, namespace, runs, jobDestination); err != nil {
			return models.JobDeploymentDetail{}, err
		}
	}
	return models.JobDeploymentDetail{}, nil
}

func (*Scheduler) DeleteJobs(context.Context, string, models.NamespaceSpec, []string, progress.Observer) error {
	return nil
}

func (*Scheduler) GetJobStatus(context.Context, models.ProjectSpec, string) ([]models.JobStatus, error) {
	panic("implement me")
}

func (*Scheduler) Clear(context.Context, models.ProjectSpec, string, time.Time, time.Time) error {
	return nil
}

func (*Scheduler) GetJobRunStatus(context.Context, models.ProjectSpec, string, time.Time, time.Time, int) ([]models.JobStatus, error) {
	panic("implement me")
}

func (*Scheduler) GetJobRuns(context.Context, models.ProjectSpec, *models.JobQuery, *cron.ScheduleSpec) ([]models.JobRun, error) {
	return []models.JobRun{}, nil
}

func NewScheduler(jobRunRepo store.JobRunRepository, nowFn func() time.Time) *Scheduler {
	return &Scheduler{
		jobRunRepo: jobRunRepo,
		Now:        nowFn,
	}
}
