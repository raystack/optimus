package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
	errs "github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/cron"
)

func TestReplayService(t *testing.T) {
	ctx := context.Background()
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	jobName := scheduler.JobName("sample_select")
	startTimeStr := "2023-01-02T15:00:00Z"
	startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
	endTime := startTime.Add(48 * time.Hour)
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
	parallel := true
	description := "sample backfill"
	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
	replayConfig := scheduler.NewReplayConfig(startTime, endTime, parallel, replayJobConfig, description)
	replayID := uuid.New()
	job := scheduler.Job{
		Name:   jobName,
		Tenant: tnnt,
	}
	jobWithDetails := &scheduler.JobWithDetails{
		Job: &job,
		JobMetadata: &scheduler.JobMetadata{
			Version: 1,
		},
		Schedule: &scheduler.Schedule{
			StartDate: startTime.Add(-time.Hour * 24),
			Interval:  "0 12 * * *",
		},
	}
	jobCronStr := "0 12 * * *"
	jobCron, _ := cron.ParseCronSchedule(jobCronStr)

	t.Run("CreateReplay", func(t *testing.T) {
		t.Run("should return replay ID if replay created successfully", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayValidator := new(ReplayValidator)
			defer replayValidator.AssertExpectations(t)

			scheduledTime1Str := "2023-01-03T12:00:00Z"
			scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTime1Str)
			scheduledTime2 := scheduledTime1.Add(24 * time.Hour)
			replayRuns := []*scheduler.JobRunStatus{
				{ScheduledAt: scheduledTime1, State: scheduler.StatePending},
				{ScheduledAt: scheduledTime2, State: scheduler.StatePending},
			}
			replayReq := scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			jobRepository.On("GetJobDetails", ctx, projName, jobName).Return(jobWithDetails, nil)
			replayValidator.On("Validate", ctx, replayReq, jobCron).Return(nil)
			replayRepository.On("RegisterReplay", ctx, replayReq, replayRuns).Return(replayID, nil)

			replayService := service.NewReplayService(replayRepository, jobRepository, replayValidator)
			result, err := replayService.CreateReplay(ctx, tnnt, jobName, replayConfig)
			assert.NoError(t, err)
			assert.Equal(t, replayID, result)
		})

		t.Run("should return error if not pass validation", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayValidator := new(ReplayValidator)
			defer replayValidator.AssertExpectations(t)

			replayReq := scheduler.NewReplayRequest(jobName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			jobRepository.On("GetJobDetails", ctx, projName, jobName).Return(jobWithDetails, nil)
			replayValidator.On("Validate", ctx, replayReq, jobCron).Return(errors.New("not passed validation"))

			replayService := service.NewReplayService(replayRepository, jobRepository, replayValidator)
			result, err := replayService.CreateReplay(ctx, tnnt, jobName, replayConfig)
			assert.ErrorContains(t, err, "not passed validation")
			assert.Equal(t, uuid.Nil, result)
		})

		t.Run("should return error if unable to get job details", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayValidator := new(ReplayValidator)
			defer replayValidator.AssertExpectations(t)

			internalErr := errors.New("internal error")
			jobRepository.On("GetJobDetails", ctx, projName, jobName).Return(nil, internalErr)

			replayService := service.NewReplayService(replayRepository, jobRepository, replayValidator)
			result, err := replayService.CreateReplay(ctx, tnnt, jobName, replayConfig)
			assert.ErrorIs(t, err, internalErr)
			assert.Equal(t, uuid.Nil, result)
		})

		t.Run("should return error if namespace name is not match", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayValidator := new(ReplayValidator)
			defer replayValidator.AssertExpectations(t)

			invalidTenant, _ := tenant.NewTenant(projName.String(), "invalid-namespace")

			jobRepository.On("GetJobDetails", ctx, projName, jobName).Return(jobWithDetails, nil)

			replayService := service.NewReplayService(replayRepository, jobRepository, replayValidator)
			result, err := replayService.CreateReplay(ctx, invalidTenant, jobName, replayConfig)
			assert.ErrorContains(t, err, "job sample_select does not exist in invalid-namespace namespace")
			assert.Equal(t, uuid.Nil, result)
		})
	})
	t.Run("GetReplayList", func(t *testing.T) {
		t.Run("should return replay list with no error", func(t *testing.T) {
			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
			replay1 := scheduler.NewReplayRequest("sample-job-A", tnnt, replayConfig, scheduler.ReplayStateInProgress)
			replay2 := scheduler.NewReplayRequest("sample-job-B", tnnt, replayConfig, scheduler.ReplayStateCreated)
			replay3 := scheduler.NewReplayRequest("sample-job-C", tnnt, replayConfig, scheduler.ReplayStateFailed)
			replays := []*scheduler.Replay{replay1, replay2, replay3}
			replayRepository := new(ReplayRepository)
			replayRepository.On("GetReplaysByProject", ctx, mock.Anything, mock.Anything).Return(replays, nil)
			defer replayRepository.AssertExpectations(t)

			replayService := service.NewReplayService(replayRepository, nil, nil)
			result, err := replayService.GetReplayList(ctx, tnnt.ProjectName())
			assert.NoError(t, err)
			assert.Len(t, result, 3)
		})

		t.Run("should return error when get replay by project is fail", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			replayRepository.On("GetReplaysByProject", ctx, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
			defer replayRepository.AssertExpectations(t)

			replayService := service.NewReplayService(replayRepository, nil, nil)
			result, err := replayService.GetReplayList(ctx, tnnt.ProjectName())
			assert.Error(t, err)
			assert.Nil(t, result)
		})
	})

	t.Run("GetReplayByID", func(t *testing.T) {
		t.Run("returns empty if replay is not found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replayID := uuid.New()
			replayRepository.On("GetReplayByID", ctx, replayID).Return(nil, errs.NotFound("entity", "not found"))

			replayService := service.NewReplayService(replayRepository, nil, nil)
			result, err := replayService.GetReplayByID(ctx, replayID)
			assert.True(t, errs.IsErrorType(err, errs.ErrNotFound))
			assert.Empty(t, result)
		})
		t.Run("returns err if get replay by id on replay repo is failed", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replayID := uuid.New()
			replayRepository.On("GetReplayByID", ctx, replayID).Return(nil, errors.New("internal error"))

			replayService := service.NewReplayService(replayRepository, nil, nil)
			result, err := replayService.GetReplayByID(ctx, replayID)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns success if replay with runs exist", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replayID := uuid.New()
			replay := scheduler.NewReplay(replayID, jobName, tnnt, replayConfig, scheduler.ReplayStateInProgress, startTime)
			replayRepository.On("GetReplayByID", ctx, replayID).Return(&scheduler.ReplayWithRun{
				Replay: replay,
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: startTime,
						State:       scheduler.StatePending,
					},
				},
			}, nil)

			replayService := service.NewReplayService(replayRepository, nil, nil)
			result, err := replayService.GetReplayByID(ctx, replayID)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NotEmpty(t, result)
		})
	})
}

// ReplayRepository is an autogenerated mock type for the ReplayRepository type
type ReplayRepository struct {
	mock.Mock
}

// GetReplaysByProject provides a mock function with given fields: ctx, projectName, dayLimits
func (_m *ReplayRepository) GetReplaysByProject(ctx context.Context, projectName tenant.ProjectName, dayLimits int) ([]*scheduler.Replay, error) {
	ret := _m.Called(ctx, projectName, dayLimits)

	var r0 []*scheduler.Replay
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, int) ([]*scheduler.Replay, error)); ok {
		return rf(ctx, projectName, dayLimits)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, int) []*scheduler.Replay); ok {
		r0 = rf(ctx, projectName, dayLimits)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.Replay)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, int) error); ok {
		r1 = rf(ctx, projectName, dayLimits)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReplayByID provides a mock function with given fields: ctx, replayID
func (_m *ReplayRepository) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	ret := _m.Called(ctx, replayID)

	var r0 *scheduler.ReplayWithRun
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) (*scheduler.ReplayWithRun, error)); ok {
		return rf(ctx, replayID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) *scheduler.ReplayWithRun); ok {
		r0 = rf(ctx, replayID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*scheduler.ReplayWithRun)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, uuid.UUID) error); ok {
		r1 = rf(ctx, replayID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReplayRequestsByStatus provides a mock function with given fields: ctx, statusList
func (_m *ReplayRepository) GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error) {
	ret := _m.Called(ctx, statusList)

	var r0 []*scheduler.Replay
	if rf, ok := ret.Get(0).(func(context.Context, []scheduler.ReplayState) []*scheduler.Replay); ok {
		r0 = rf(ctx, statusList)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.Replay)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, []scheduler.ReplayState) error); ok {
		r1 = rf(ctx, statusList)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReplayToExecute provides a mock function with given fields: _a0
func (_m *ReplayRepository) GetReplayToExecute(_a0 context.Context) (*scheduler.ReplayWithRun, error) {
	ret := _m.Called(_a0)

	var r0 *scheduler.ReplayWithRun
	if rf, ok := ret.Get(0).(func(context.Context) *scheduler.ReplayWithRun); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*scheduler.ReplayWithRun)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RegisterReplay provides a mock function with given fields: ctx, replay, runs
func (_m *ReplayRepository) RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error) {
	ret := _m.Called(ctx, replay, runs)

	var r0 uuid.UUID
	if rf, ok := ret.Get(0).(func(context.Context, *scheduler.Replay, []*scheduler.JobRunStatus) uuid.UUID); ok {
		r0 = rf(ctx, replay, runs)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(uuid.UUID)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *scheduler.Replay, []*scheduler.JobRunStatus) error); ok {
		r1 = rf(ctx, replay, runs)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateReplay provides a mock function with given fields: ctx, replayID, state, runs, message
func (_m *ReplayRepository) UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error {
	ret := _m.Called(ctx, replayID, state, runs, message)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, scheduler.ReplayState, []*scheduler.JobRunStatus, string) error); ok {
		r0 = rf(ctx, replayID, state, runs, message)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// UpdateReplayStatus provides a mock function with given fields: ctx, replayID, state, message
func (_m *ReplayRepository) UpdateReplayStatus(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, message string) error {
	ret := _m.Called(ctx, replayID, state, message)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID, scheduler.ReplayState, string) error); ok {
		r0 = rf(ctx, replayID, state, message)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetReplayJobConfig provides a mock function with given fields: ctx, jobTenant, jobName, scheduledAt
func (_m *ReplayRepository) GetReplayJobConfig(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (map[string]string, error) {
	ret := _m.Called(ctx, jobTenant, jobName, scheduledAt)

	var r0 map[string]string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time) (map[string]string, error)); ok {
		return rf(ctx, jobTenant, jobName, scheduledAt)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time) map[string]string); ok {
		r0 = rf(ctx, jobTenant, jobName, scheduledAt)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time) error); ok {
		r1 = rf(ctx, jobTenant, jobName, scheduledAt)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReplayValidator is an autogenerated mock type for the ReplayValidator type
type ReplayValidator struct {
	mock.Mock
}

// Validate provides a mock function with given fields: ctx, replayRequest, jobCron
func (_m *ReplayValidator) Validate(ctx context.Context, replayRequest *scheduler.Replay, jobCron *cron.ScheduleSpec) error {
	ret := _m.Called(ctx, replayRequest, jobCron)
	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *scheduler.Replay, *cron.ScheduleSpec) error); ok {
		r0 = rf(ctx, replayRequest, jobCron)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
