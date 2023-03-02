package service

import (
	"fmt"

	"github.com/google/uuid"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/lib/cron"
)

type ReplayRepository interface {
	RegisterReplay(ctx context.Context, replay *scheduler.Replay) (uuid.UUID, error)
	UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error

	GetReplayToExecute(context.Context) (*scheduler.StoredReplay, error)
	GetReplayByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.StoredReplay, error)
}

type ReplayValidator interface {
	Validate(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig, jobCron *cron.ScheduleSpec) error
}

type ReplayService struct {
	replayRepo ReplayRepository
	jobRepo    JobRepository

	validator ReplayValidator
}

func (r ReplayService) CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	subjectJob, err := r.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		return uuid.Nil, err
	}

	jobCron, err := cron.ParseCronSchedule(subjectJob.Schedule.Interval)
	if err != nil {
		return uuid.Nil, fmt.Errorf("unable to parse job cron interval: %w", err)
	}

	if err := r.validator.Validate(ctx, tenant, jobName, config, jobCron); err != nil {
		return uuid.Nil, err
	}

	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)

	replay := scheduler.NewReplay(jobName, tenant, config, runs, scheduler.ReplayStateCreated)

	return r.replayRepo.RegisterReplay(ctx, replay)
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, validator ReplayValidator) *ReplayService {
	return &ReplayService{replayRepo: replayRepo, jobRepo: jobRepo, validator: validator}
}
