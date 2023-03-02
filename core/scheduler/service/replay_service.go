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
	GetReplayToExecute(context.Context) (*scheduler.StoredReplay, error)
	UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error
}

type ReplayValidator interface {
	Validate(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) error
}

type ReplayService struct {
	replayRepo ReplayRepository
	jobRepo    JobRepository

	validator ReplayValidator
}

func (r ReplayService) CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	// 1. Validate
	if err := r.validator.Validate(ctx, tenant, jobName, config); err != nil {
		return uuid.Nil, err
	}

	// 2. Calculate replay runs
	//    TODO: the goal for now is only to fetch the job interval. consider creating a new query in repository.
	subjectJob, err := r.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		return uuid.Nil, err
	}

	// TODO: are we expecting users to fill start time & end time of execution/scheduled?
	jobCron, err := cron.ParseCronSchedule(subjectJob.Schedule.Interval)
	if err != nil {
		return uuid.Nil, fmt.Errorf("unable to parse job cron interval: %w", err)
	}
	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)

	// 3. Initialize replay
	replay := scheduler.NewReplay(jobName, tenant, config, runs, scheduler.ReplayStateCreated)

	// 4. Store
	return r.replayRepo.RegisterReplay(ctx, replay)
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, validator ReplayValidator) *ReplayService {
	return &ReplayService{replayRepo: replayRepo, jobRepo: jobRepo, validator: validator}
}
