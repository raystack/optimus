package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/lib/cron"
)

func TestReplayWorker(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	jobAName, _ := scheduler.JobNameFrom("job-a")
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
	startTimeStr := "2023-01-02T00:00:00Z"
	startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
	endTime := startTime.Add(48 * time.Hour)
	replayDescription := "sample backfill"
	scheduledTimeStr1 := "2023-01-02T12:00:00Z"
	scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTimeStr1)
	runsCriteriaJobA := &scheduler.JobRunsCriteria{
		Name:      jobAName.String(),
		StartDate: startTime,
		EndDate:   endTime,
	}
	scheduledTime2 := scheduledTime1.Add(24 * time.Hour)
	executionTime1 := scheduledTime1.Add(-24 * time.Hour)
	executionTime2 := executionTime1.Add(24 * time.Hour)

	t.Run("Process", func(t *testing.T) {
		t.Run("should able to process new sequential replay request with single run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			replayReqs := []*scheduler.StoredReplay{
				{
					ID: uuid.New(),
					Replay: &scheduler.Replay{
						JobName: jobAName,
						Tenant:  tnnt,
						Config:  scheduler.NewReplayConfig(startTime, endTime, false, replayDescription),
						Runs: []*scheduler.JobRunStatus{
							{
								ScheduledAt: scheduledTime1,
								State:       scheduler.StatePending,
								LogicalTime: executionTime1,
							},
						},
						State: scheduler.ReplayStateCreated,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateQueued,
					LogicalTime: executionTime1,
				},
			}

			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateInProgress, replayReqs[0].Replay.Runs, "").Return(nil)
			sch.On("Clear", ctx, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour)).Return(nil)
			jobRunService.On("GetJobRuns", ctx, projName, jobAName, runsCriteriaJobA).Return(updatedRuns, nil)
			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRunService)
			replayWorker.Process(ctx, replayReqs)
		})
		t.Run("should able to process new sequential replay request with multiple run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			replayReqs := []*scheduler.StoredReplay{
				{
					ID: uuid.New(),
					Replay: &scheduler.Replay{
						JobName: jobAName,
						Tenant:  tnnt,
						Config:  scheduler.NewReplayConfig(startTime, endTime, false, replayDescription),
						Runs: []*scheduler.JobRunStatus{
							{
								ScheduledAt: scheduledTime1,
								State:       scheduler.StatePending,
								LogicalTime: executionTime1,
							},
							{
								ScheduledAt: scheduledTime2,
								State:       scheduler.StatePending,
								LogicalTime: executionTime2,
							},
						},
						State: scheduler.ReplayStateCreated,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateQueued,
					LogicalTime: executionTime1,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StatePending,
					LogicalTime: executionTime2,
				},
			}

			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateInProgress, replayReqs[0].Replay.Runs, "").Return(nil)
			sch.On("Clear", ctx, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour)).Return(nil)
			jobRunService.On("GetJobRuns", ctx, projName, jobAName, runsCriteriaJobA).Return(updatedRuns, nil)
			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStatePartialReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRunService)
			replayWorker.Process(ctx, replayReqs)
		})
		t.Run("should able to process new parallel replay request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			replayReqs := []*scheduler.StoredReplay{
				{
					ID: uuid.New(),
					Replay: &scheduler.Replay{
						JobName: jobAName,
						Tenant:  tnnt,
						Config:  scheduler.NewReplayConfig(startTime, endTime, true, replayDescription),
						Runs: []*scheduler.JobRunStatus{
							{
								ScheduledAt: scheduledTime1,
								State:       scheduler.StatePending,
								LogicalTime: executionTime1,
							},
							{
								ScheduledAt: scheduledTime2,
								State:       scheduler.StatePending,
								LogicalTime: executionTime2,
							},
						},
						State: scheduler.ReplayStateCreated,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateQueued,
					LogicalTime: executionTime1,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StatePending,
					LogicalTime: executionTime2,
				},
			}

			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateInProgress, replayReqs[0].Replay.Runs, "").Return(nil)
			sch.On("ClearBatch", ctx, tnnt, jobAName, executionTime1, executionTime2).Return(nil)
			jobRunService.On("GetJobRuns", ctx, projName, jobAName, runsCriteriaJobA).Return(updatedRuns, nil)
			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRunService)
			replayWorker.Process(ctx, replayReqs)
		})

		t.Run("should able to process partial replayed request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			replayReqs := []*scheduler.StoredReplay{
				{
					ID: uuid.New(),
					Replay: &scheduler.Replay{
						JobName: jobAName,
						Tenant:  tnnt,
						Config:  scheduler.NewReplayConfig(startTime, endTime, true, replayDescription),
						Runs: []*scheduler.JobRunStatus{
							{
								ScheduledAt: scheduledTime1,
								State:       scheduler.StateQueued,
								LogicalTime: executionTime1,
							},
							{
								ScheduledAt: scheduledTime2,
								State:       scheduler.StatePending,
								LogicalTime: executionTime2,
							},
						},
						State: scheduler.ReplayStatePartialReplayed,
					},
				},
			}
			updatedRuns1 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
					LogicalTime: executionTime1,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StatePending,
					LogicalTime: executionTime2,
				},
			}
			updatedRuns2 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
					LogicalTime: executionTime1,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateQueued,
					LogicalTime: executionTime2,
				},
			}

			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateInProgress, replayReqs[0].Replay.Runs, "").Return(nil).Once()
			jobRunService.On("GetJobRuns", ctx, projName, jobAName, runsCriteriaJobA).Return(updatedRuns1, nil).Once()
			sch.On("Clear", ctx, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour)).Return(nil)
			jobRunService.On("GetJobRuns", ctx, projName, jobAName, runsCriteriaJobA).Return(updatedRuns2, nil).Once()
			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateReplayed, updatedRuns2, "").Return(nil).Once()

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRunService)
			replayWorker.Process(ctx, replayReqs)
		})

		t.Run("should able to process replayed request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRunService := new(mockJobRunService)
			defer jobRunService.AssertExpectations(t)

			replayReqs := []*scheduler.StoredReplay{
				{
					ID: uuid.New(),
					Replay: &scheduler.Replay{
						JobName: jobAName,
						Tenant:  tnnt,
						Config:  scheduler.NewReplayConfig(startTime, endTime, true, replayDescription),
						Runs: []*scheduler.JobRunStatus{
							{
								ScheduledAt: scheduledTime1,
								State:       scheduler.StateSuccess,
								LogicalTime: executionTime1,
							},
							{
								ScheduledAt: scheduledTime2,
								State:       scheduler.StateQueued,
								LogicalTime: executionTime2,
							},
						},
						State: scheduler.ReplayStateReplayed,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
					LogicalTime: executionTime1,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateSuccess,
					LogicalTime: executionTime2,
				},
			}

			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateInProgress, replayReqs[0].Replay.Runs, "").Return(nil)
			jobRunService.On("GetJobRuns", ctx, projName, jobAName, runsCriteriaJobA).Return(updatedRuns, nil)
			replayRepository.On("UpdateReplay", ctx, replayReqs[0].ID, scheduler.ReplayStateSuccess, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRunService)
			replayWorker.Process(ctx, replayReqs)
		})
	})
}

// mockReplayScheduler is an autogenerated mock type for the mockReplayScheduler type
type mockReplayScheduler struct {
	mock.Mock
}

// Clear provides a mock function with given fields: ctx, t, jobName, scheduledAt
func (_m *mockReplayScheduler) Clear(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) error {
	ret := _m.Called(ctx, t, jobName, scheduledAt)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time) error); ok {
		r0 = rf(ctx, t, jobName, scheduledAt)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ClearBatch provides a mock function with given fields: ctx, t, jobName, startTime, endTime
func (_m *mockReplayScheduler) ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime time.Time, endTime time.Time) error {
	ret := _m.Called(ctx, t, jobName, startTime, endTime)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time, time.Time) error); ok {
		r0 = rf(ctx, t, jobName, startTime, endTime)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetJobRuns provides a mock function with given fields: ctx, t, criteria, jobCron
func (_m *mockReplayScheduler) GetJobRuns(ctx context.Context, t tenant.Tenant, criteria *scheduler.JobRunsCriteria, jobCron *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	ret := _m.Called(ctx, t, criteria, jobCron)

	var r0 []*scheduler.JobRunStatus
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, *scheduler.JobRunsCriteria, *cron.ScheduleSpec) []*scheduler.JobRunStatus); ok {
		r0 = rf(ctx, t, criteria, jobCron)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.JobRunStatus)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, *scheduler.JobRunsCriteria, *cron.ScheduleSpec) error); ok {
		r1 = rf(ctx, t, criteria, jobCron)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockJobRunService is an autogenerated mock type for the JobReplayRunService type
type mockJobRunService struct {
	mock.Mock
}

// GetJobRuns provides a mock function with given fields: ctx, projectName, jobName, criteria
func (_m *mockJobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	ret := _m.Called(ctx, projectName, jobName, criteria)

	var r0 []*scheduler.JobRunStatus
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName, scheduler.JobName, *scheduler.JobRunsCriteria) []*scheduler.JobRunStatus); ok {
		r0 = rf(ctx, projectName, jobName, criteria)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.JobRunStatus)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName, scheduler.JobName, *scheduler.JobRunsCriteria) error); ok {
		r1 = rf(ctx, projectName, jobName, criteria)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
