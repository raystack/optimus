//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	postgres "github.com/odpf/optimus/internal/store/postgres/scheduler"
)

func TestPostgresSchedulerRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	endTime := time.Now()
	startTime := endTime.Add(-48 * time.Hour)
	description := "sample backfill"

	jobRunsAllPending := []*scheduler.JobRunStatus{
		{
			ScheduledAt: startTime,
			State:       scheduler.StatePending,
		},
		{
			ScheduledAt: startTime.Add(24 * time.Hour),
			State:       scheduler.StatePending,
		},
	}
	jobRunsAllQueued := []*scheduler.JobRunStatus{
		{
			ScheduledAt: startTime,
			State:       scheduler.StateQueued,
		},
		{
			ScheduledAt: startTime.Add(24 * time.Hour),
			State:       scheduler.StateQueued,
		},
	}

	t.Run("RegisterReplay", func(t *testing.T) {
		t.Run("store replay request and the runs", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, description)
			replayReq := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			replayID, err := replayRepo.RegisterReplay(ctx, replayReq, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID)
		})
	})

	t.Run("UpdateReplay", func(t *testing.T) {
		t.Run("updates replay request and reinsert the runs", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, description)
			replayReq := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			replayID, err := replayRepo.RegisterReplay(ctx, replayReq, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID)

			err = replayRepo.UpdateReplay(ctx, replayID, scheduler.ReplayStateReplayed, jobRunsAllQueued, "")
			assert.NoError(t, err)
		})
	})

	t.Run("GetReplayToExecute", func(t *testing.T) {
		t.Run("get executable replay", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, description)
			replayReq1 := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateSuccess)
			replayReq2 := scheduler.NewReplayRequest(jobBName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			replayID1, err := replayRepo.RegisterReplay(ctx, replayReq1, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID1)

			replayID2, err := replayRepo.RegisterReplay(ctx, replayReq2, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID2)

			replayToExecute, err := replayRepo.GetReplayToExecute(ctx)
			assert.Nil(t, err)
			assert.Equal(t, jobBName, replayToExecute.Replay.JobName.String())
		})
	})

	t.Run("GetReplayRequestByStatus", func(t *testing.T) {
		t.Run("return replay requests given list of status", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, description)
			replayReq1 := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress)
			replayReq2 := scheduler.NewReplayRequest(jobBName, tnnt, replayConfig, scheduler.ReplayStateCreated)
			replayReq3 := scheduler.NewReplayRequest("sample-job-C", tnnt, replayConfig, scheduler.ReplayStateFailed)

			replayID1, err := replayRepo.RegisterReplay(ctx, replayReq1, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID1)

			replayID2, err := replayRepo.RegisterReplay(ctx, replayReq2, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID2)

			replayID3, err := replayRepo.RegisterReplay(ctx, replayReq3, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID3)

			replayReqs, err := replayRepo.GetReplayRequestByStatus(ctx, []scheduler.ReplayState{scheduler.ReplayStateCreated, scheduler.ReplayStateInProgress})
			assert.Nil(t, err)
			assert.EqualValues(t, []string{jobAName, jobBName}, []string{replayReqs[0].JobName.String(), replayReqs[1].JobName.String()})
		})
	})
}
