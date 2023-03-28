package service_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"golang.org/x/net/context"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/scheduler/service"
	"github.com/goto/optimus/core/tenant"
)

func TestReplayManager(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	currentTime := func() time.Time { return time.Now() }
	conf := config.ReplayConfig{ReplayTimeout: time.Hour * 3}
	replaysToCheck := []scheduler.ReplayState{
		scheduler.ReplayStateCreated, scheduler.ReplayStateInProgress,
		scheduler.ReplayStatePartialReplayed, scheduler.ReplayStateReplayed,
	}
	replayID := uuid.New()
	jobName := scheduler.JobName("sample_select")
	replayStartTimeStr := "2023-01-03T12:00:00Z"
	replayStartTime, _ := time.Parse(scheduler.ISODateFormat, replayStartTimeStr)
	replayEndTime := replayStartTime.Add(24 * time.Hour)
	replayDescription := "for backfill"
	replayReqConf := scheduler.NewReplayConfig(replayStartTime, replayEndTime, false, map[string]string{}, replayDescription)
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())

	t.Run("StartReplayLoop", func(t *testing.T) {
		t.Run("should not proceed on the timeout process if unable to get replay requests by status", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			err := errors.New("internal error")
			replayRepository.On("GetReplayRequestsByStatus", ctx, replaysToCheck).Return(nil, err)
			replayRepository.On("GetReplayToExecute", ctx).Return(nil, err)

			replayManager := service.NewReplayManager(logger, replayRepository, nil, currentTime, conf)
			replayManager.StartReplayLoop()
		})
		t.Run("should mark replay request as failed if it is timed out", func(t *testing.T) {
			replayRepository := new(ReplayRepository)
			defer replayRepository.AssertExpectations(t)

			replayCreatedTime1 := time.Now().Add(-24 * time.Hour)
			replayCreatedTime2 := time.Now().Add(-1 * time.Hour)

			replayReq1 := scheduler.NewReplay(replayID, jobName, tnnt, replayReqConf, scheduler.ReplayStateInProgress, replayCreatedTime1)
			replayReq2 := scheduler.NewReplay(uuid.New(), "other_job", tnnt, replayReqConf, scheduler.ReplayStateInProgress, replayCreatedTime2)

			replayRepository.On("GetReplayRequestsByStatus", ctx, replaysToCheck).Return([]*scheduler.Replay{replayReq1, replayReq2}, nil)
			replayRepository.On("UpdateReplayStatus", ctx, replayID, scheduler.ReplayStateFailed, "replay timed out").Return(nil).Once()

			err := errors.New("internal error")
			replayRepository.On("GetReplayToExecute", ctx).Return(nil, err)

			replayManager := service.NewReplayManager(logger, replayRepository, nil, currentTime, conf)
			replayManager.StartReplayLoop()
		})
	})
}
