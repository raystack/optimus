package service

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
	"github.com/goto/optimus/internal/telemetry"
)

const (
	getReplaysDayLimit = 30 // TODO: make it configurable via cli

	metricJobReplay = "jobrun_replay_requests_total"
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

	logger log.Logger
}

func (r ReplayService) CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	subjectJob, err := r.jobRepo.GetJobDetails(ctx, tenant.ProjectName(), jobName)
	if err != nil {
		r.logger.Error("error getting job details of [%s]: %s", jobName.String(), err)
		return uuid.Nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("unable to get job details for jobName: %s, project:%s", jobName, tenant.ProjectName().String()))
	}

	if subjectJob.Job.Tenant.NamespaceName() != tenant.NamespaceName() {
		r.logger.Error("job [%s] resides in namespace [%s], expecting it under [%s]", jobName, subjectJob.Job.Tenant.NamespaceName(), tenant.NamespaceName())
		return uuid.Nil, errors.InvalidArgument(scheduler.EntityReplay, fmt.Sprintf("job %s does not exist in %s namespace", jobName, tenant.NamespaceName().String()))
	}

	jobCron, err := cron.ParseCronSchedule(subjectJob.Schedule.Interval)
	if err != nil {
		r.logger.Error("error parsing cron schedule for interval [%s]: %s", subjectJob.Schedule.Interval, err)
		return uuid.Nil, errors.InternalError(scheduler.EntityReplay, "invalid cron interval for "+jobName.String(), err)
	}

	replayReq := scheduler.NewReplayRequest(jobName, tenant, config, scheduler.ReplayStateCreated)
	if err := r.validator.Validate(ctx, replayReq, jobCron); err != nil {
		r.logger.Error("error validating replay request: %s", err)
		return uuid.Nil, err
	}

	runs := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	replayID, err = r.replayRepo.RegisterReplay(ctx, replayReq, runs)
	if err != nil {
		return uuid.Nil, err
	}

	telemetry.NewCounter(metricJobReplay, map[string]string{
		"project":   tenant.ProjectName().String(),
		"namespace": tenant.NamespaceName().String(),
		"job":       jobName.String(),
		"status":    replayReq.State().String(),
	}).Inc()
	return replayID, nil
}

func (r ReplayService) GetReplayList(ctx context.Context, projectName tenant.ProjectName) (replays []*scheduler.Replay, err error) {
	return r.replayRepo.GetReplaysByProject(ctx, projectName, getReplaysDayLimit)
}

func (r ReplayService) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	replayWithRun, err := r.replayRepo.GetReplayByID(ctx, replayID)
	if err != nil {
		return nil, err
	}
	replayWithRun.Runs = scheduler.JobRunStatusList(replayWithRun.Runs).GetSortedRunsByScheduledAt()
	return replayWithRun, nil
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, validator ReplayValidator, logger log.Logger) *ReplayService {
	return &ReplayService{replayRepo: replayRepo, jobRepo: jobRepo, validator: validator, logger: logger}
}
