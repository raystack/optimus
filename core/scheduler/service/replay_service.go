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

type SchedulerRunGetter interface {
	GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error)
}

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
	runGetter  SchedulerRunGetter

	validator ReplayValidator

	logger log.Logger
}

func (r *ReplayService) CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error) {
	jobCron, err := getJobCron(ctx, r.logger, r.jobRepo, tenant, jobName)
	if err != nil {
		r.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return uuid.Nil, err
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

func (r *ReplayService) GetReplayList(ctx context.Context, projectName tenant.ProjectName) (replays []*scheduler.Replay, err error) {
	return r.replayRepo.GetReplaysByProject(ctx, projectName, getReplaysDayLimit)
}

func (r *ReplayService) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	replayWithRun, err := r.replayRepo.GetReplayByID(ctx, replayID)
	if err != nil {
		return nil, err
	}
	replayWithRun.Runs = scheduler.JobRunStatusList(replayWithRun.Runs).GetSortedRunsByScheduledAt()
	return replayWithRun, nil
}

func (r *ReplayService) GetRunsStatus(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) ([]*scheduler.JobRunStatus, error) {
	jobRunCriteria := &scheduler.JobRunsCriteria{
		Name:      jobName.String(),
		StartDate: config.StartTime,
		EndDate:   config.EndTime,
	}
	jobCron, err := getJobCron(ctx, r.logger, r.jobRepo, tenant, jobName)
	if err != nil {
		r.logger.Error("unable to get cron value for job [%s]: %s", jobName.String(), err.Error())
		return nil, err
	}
	existingRuns, err := r.runGetter.GetJobRuns(ctx, tenant, jobRunCriteria, jobCron)
	if err != nil {
		return nil, err
	}
	expectedRuns := getExpectedRuns(jobCron, config.StartTime, config.EndTime)
	tobeCreatedRuns := getMissingRuns(expectedRuns, existingRuns)
	tobeCreatedRuns = scheduler.JobRunStatusList(tobeCreatedRuns).OverrideWithStatus(scheduler.StateMissing)
	runs := tobeCreatedRuns
	runs = append(runs, existingRuns...)
	runs = scheduler.JobRunStatusList(runs).GetSortedRunsByScheduledAt()
	return runs, nil
}

func NewReplayService(replayRepo ReplayRepository, jobRepo JobRepository, validator ReplayValidator, runGetter SchedulerRunGetter, logger log.Logger) *ReplayService {
	return &ReplayService{replayRepo: replayRepo, jobRepo: jobRepo, validator: validator, runGetter: runGetter, logger: logger}
}

func getJobCron(ctx context.Context, l log.Logger, jobRepo JobRepository, tnnt tenant.Tenant, jobName scheduler.JobName) (*cron.ScheduleSpec, error) {
	jobWithDetails, err := jobRepo.GetJobDetails(ctx, tnnt.ProjectName(), jobName)
	if err != nil || jobWithDetails == nil {
		return nil, errors.AddErrContext(err, scheduler.EntityReplay,
			fmt.Sprintf("unable to get job details for jobName: %s, project: %s", jobName, tnnt.ProjectName()))
	}

	if jobWithDetails.Job.Tenant.NamespaceName() != tnnt.NamespaceName() {
		l.Error("job [%s] resides in namespace [%s], expecting it under [%s]", jobName, jobWithDetails.Job.Tenant.NamespaceName(), tnnt.NamespaceName())
		return nil, errors.InvalidArgument(scheduler.EntityReplay, fmt.Sprintf("job %s does not exist in %s namespace", jobName, tnnt.NamespaceName().String()))
	}

	interval := jobWithDetails.Schedule.Interval
	if interval == "" {
		l.Error("job interval is empty")
		return nil, errors.InvalidArgument(scheduler.EntityReplay, "job schedule interval is empty")
	}
	jobCron, err := cron.ParseCronSchedule(interval)
	if err != nil {
		l.Error("error parsing cron interval: %s", err)
		return nil, errors.InternalError(scheduler.EntityReplay, "unable to parse job cron interval", err)
	}
	return jobCron, nil
}
