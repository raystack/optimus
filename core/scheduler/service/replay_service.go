package service

import (
	"fmt"

	"github.com/google/uuid"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
)

const (
	getReplaysDayLimit = 30 // TODO: make it configurable via cli
)

type ReplayRepository interface {
	RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error)
	UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error
	UpdateReplayStatus(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, message string) error

	GetReplayToExecute(context.Context) (*scheduler.ReplayWithRun, error)
	GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error)
	GetReplaysByProject(ctx context.Context, projectName tenant.ProjectName, dayLimits int) ([]*scheduler.Replay, error)
	GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error)
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
		return uuid.Nil, fmt.Errorf("encountered unexpected error when parsing job cron interval for job %s: %w", jobName, err)
	}

	replayReq := scheduler.NewReplayRequest(jobName, tenant, config, scheduler.ReplayStateCreated)
	if err := r.validator.Validate(ctx, replayReq, jobCron); err != nil {
		return uuid.Nil, err
	}

	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	return r.replayRepo.RegisterReplay(ctx, replayReq, runs)
}

func (r ReplayService) GetReplayList(ctx context.Context, projectName tenant.ProjectName) (replays []*scheduler.Replay, err error) {
	return r.replayRepo.GetReplaysByProject(ctx, projectName, getReplaysDayLimit)
}

func (r ReplayService) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	return r.replayRepo.GetReplayByID(ctx, replayID)
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, validator ReplayValidator) *ReplayService {
	return &ReplayService{replayRepo: replayRepo, jobRepo: jobRepo, validator: validator}
}
