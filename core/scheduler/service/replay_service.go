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
	RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error)
	UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error
	UpdateReplayStatus(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, message string) error

	GetReplayToExecute(context.Context) (*scheduler.ReplayWithRun, error)
	GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error)
}

type ReplayValidator interface {
	Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error
}

type ReplayService struct {
	replayRepo ReplayRepository
	jobRepo    JobRepository

	validator ReplayValidator
}

func (r ReplayService) CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	subjectJob, err := r.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		return uuid.Nil, fmt.Errorf("unable to get job details from DB for jobName: %s, project:%s,  error:%w ", jobName, tenant.ProjectName().String(), err)
	}

	jobCron, err := cron.ParseCronSchedule(subjectJob.Schedule.Interval)
	if err != nil {
		return uuid.Nil, fmt.Errorf("unable to parse job cron interval for job %s: %w", jobName, err)
	}

	replayReq := scheduler.NewReplayRequest(jobName, tenant, config, scheduler.ReplayStateCreated)
	if err := r.validator.Validate(ctx, replayReq, jobCron); err != nil {
		return uuid.Nil, err
	}

	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	return r.replayRepo.RegisterReplay(ctx, replayReq, runs)
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, validator ReplayValidator) *ReplayService {
	return &ReplayService{replayRepo: replayRepo, jobRepo: jobRepo, validator: validator}
}
