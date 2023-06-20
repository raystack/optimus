package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/cron"
)

func TestReplayWorker(t *testing.T) {
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
	scheduledTime3 := scheduledTime2.Add(24 * time.Hour)
	executionTime1 := scheduledTime1.Add(-24 * time.Hour)
	executionTime2 := executionTime1.Add(24 * time.Hour)
	jobCronStr := "0 12 * * *"
	jobA := scheduler.Job{
		Name:   jobAName,
		Tenant: tnnt,
	}
	jobAWithDetails := &scheduler.JobWithDetails{
		Job: &jobA,
		JobMetadata: &scheduler.JobMetadata{
			Version: 1,
		},
		Schedule: &scheduler.Schedule{
			StartDate: startTime.Add(-time.Hour * 24),
			Interval:  jobCronStr,
		},
	}
	jobCron, _ := cron.ParseCronSchedule(jobCronStr)
	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
	replayConfig := scheduler.NewReplayConfig(startTime, endTime, false, replayJobConfig, replayDescription)
	replayConfigParallel := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, replayDescription)
	replayServerConfig := config.ReplayConfig{}
	internalErr := errors.New("internal error")

	t.Run("Process", func(t *testing.T) {
		t.Run("should able to process new sequential replay request with single run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateInProgress,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(replayReq.Runs, nil)
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour)).Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to process new sequential replay request with multiple run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateInProgress,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StatePending,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(replayReq.Runs, nil)
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour)).Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStatePartialReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to process new parallel replay request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(replayReq.Runs, nil)
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, executionTime1, executionTime2).Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateReplayed, mock.Anything, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to process new replay request with creating non existing runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
				},
			}
			updatedRunsAfterRunCreate := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateInProgress,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StatePending,
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateInProgress,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateInProgress,
				},
			}
			existingRuns := []*scheduler.JobRunStatus{{
				ScheduledAt: scheduledTime2,
				State:       scheduler.StateSuccess,
			}}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(existingRuns, nil)
			sch.On("CreateRun", mock.Anything, tnnt, jobAName, scheduledTime1, "replayed").Return(nil).Once()
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour)).Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStatePartialReplayed, updatedRunsAfterRunCreate, "").Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStatePartialReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})

		t.Run("should able to update replay state as failed if unable to get job details", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(nil, internalErr)
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything).Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to update replay state as failed if unable to do clear batch of runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(replayReq.Runs, nil)
			sch.On("ClearBatch", mock.Anything, tnnt, jobAName, executionTime1, executionTime2).Return(internalErr)
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything).Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to update replay state as failed if unable to do clear run", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StatePending,
					},
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, mock.Anything, jobCron).Return(replayReq.Runs, nil)
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime1.Add(-24*time.Hour)).Return(internalErr)
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything).Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})

		t.Run("should able to process partial replayed request with the recent run status is success", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStatePartialReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateInProgress,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime3,
						State:       scheduler.StatePending,
					},
				},
			}
			updatedRuns1 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateFailed,
				},
				{
					ScheduledAt: scheduledTime3,
					State:       scheduler.StateFailed,
				},
			}
			updatedRuns2 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateInProgress,
				},
				{
					ScheduledAt: scheduledTime3,
					State:       scheduler.StatePending,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(updatedRuns1, nil).Once()
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour)).Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStatePartialReplayed, updatedRuns2, "").Return(nil).Once()

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to process partial replayed request with the recent run status is failed", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStatePartialReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateInProgress,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime3,
						State:       scheduler.StatePending,
					},
				},
			}
			updatedRuns1 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateFailed,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateFailed,
				},
				{
					ScheduledAt: scheduledTime3,
					State:       scheduler.StateFailed,
				},
			}
			updatedRuns2 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateFailed,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateInProgress,
				},
				{
					ScheduledAt: scheduledTime3,
					State:       scheduler.StatePending,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(updatedRuns1, nil).Once()
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour)).Return(nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStatePartialReplayed, updatedRuns2, "").Return(nil).Once()

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to update replay state as failed if unable to fetch job runs", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStatePartialReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateInProgress,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
					{
						ScheduledAt: scheduledTime3,
						State:       scheduler.StatePending,
					},
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(nil, internalErr).Once()
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything).Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to update replay state as failed if unable to clear run when processing partial replayed request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStatePartialReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateInProgress,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StatePending,
					},
				},
			}
			updatedRuns1 := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StatePending,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(updatedRuns1, nil).Once()
			sch.On("Clear", mock.Anything, tnnt, jobAName, scheduledTime2.Add(-24*time.Hour)).Return(internalErr)
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything).Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})

		t.Run("should able to process replayed request if all state are success", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateSuccess,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StateInProgress,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateSuccess,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(updatedRuns, nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateSuccess, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to process replayed request if some of the runs are in failed state", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateInProgress,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StateInProgress,
					},
					{
						ScheduledAt: scheduledTime3,
						State:       scheduler.StateInProgress,
					},
				},
			}
			runsFromScheduler := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateFailed,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateRunning,
				},
				{
					ScheduledAt: scheduledTime3,
					State:       scheduler.StateRunning,
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateFailed,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateInProgress,
				},
				{
					ScheduledAt: scheduledTime3,
					State:       scheduler.StateInProgress,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(runsFromScheduler, nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateReplayed, updatedRuns, "").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to update replay state as failed if unable to fetch runs when processing replayed request", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateSuccess,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StateInProgress,
					},
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(nil, internalErr)
			replayRepository.On("UpdateReplayStatus", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, mock.Anything).Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
		})
		t.Run("should able to update replay state as failed if all runs finished and failure found", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			sch := new(mockReplayScheduler)
			defer sch.AssertExpectations(t)

			jobRepository := new(JobRepository)
			defer jobRepository.AssertExpectations(t)

			replayReq := &scheduler.ReplayWithRun{
				Replay: scheduler.NewReplay(uuid.New(), jobAName, tnnt, replayConfigParallel, scheduler.ReplayStateReplayed, time.Now()),
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: scheduledTime1,
						State:       scheduler.StateSuccess,
					},
					{
						ScheduledAt: scheduledTime2,
						State:       scheduler.StateInProgress,
					},
				},
			}
			updatedRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: scheduledTime1,
					State:       scheduler.StateSuccess,
				},
				{
					ScheduledAt: scheduledTime2,
					State:       scheduler.StateFailed,
				},
			}

			jobRepository.On("GetJobDetails", mock.Anything, projName, jobAName).Return(jobAWithDetails, nil)
			sch.On("GetJobRuns", mock.Anything, tnnt, runsCriteriaJobA, jobCron).Return(updatedRuns, nil)
			replayRepository.On("UpdateReplay", mock.Anything, replayReq.Replay.ID(), scheduler.ReplayStateFailed, updatedRuns, "found 1 failed runs.").Return(nil)

			replayWorker := service.NewReplayWorker(logger, replayRepository, sch, jobRepository, replayServerConfig)
			replayWorker.Process(replayReq)
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
func (_m *mockReplayScheduler) ClearBatch(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, startTime, endTime time.Time) error {
	ret := _m.Called(ctx, t, jobName, startTime, endTime)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time, time.Time) error); ok {
		r0 = rf(ctx, t, jobName, startTime, endTime)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateRun provides a mock function with given fields: ctx, tnnt, jobName, executionTime, dagRunID
func (_m *mockReplayScheduler) CreateRun(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName, executionTime time.Time, dagRunIDPrefix string) error {
	ret := _m.Called(ctx, tnnt, jobName, executionTime, dagRunIDPrefix)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, time.Time, string) error); ok {
		r0 = rf(ctx, tnnt, jobName, executionTime, dagRunIDPrefix)
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
