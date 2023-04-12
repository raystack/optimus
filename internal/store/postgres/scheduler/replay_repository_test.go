//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	postgres "github.com/goto/optimus/internal/store/postgres/scheduler"
)

func TestPostgresSchedulerRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	endTime := time.Now()
	startTime := endTime.Add(-48 * time.Hour)
	replayJobConfig := map[string]string{"EXECUTION_PROJECT": "example_project"}
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

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
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

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
			replayReq := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateCreated)

			replayID, err := replayRepo.RegisterReplay(ctx, replayReq, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID)

			err = replayRepo.UpdateReplay(ctx, replayID, scheduler.ReplayStateReplayed, jobRunsAllQueued, "")
			assert.NoError(t, err)
		})
	})

	t.Run("GetReplayToExecute", func(t *testing.T) {
		t.Run("return executable replay", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
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
			assert.Equal(t, jobBName, replayToExecute.Replay.JobName().String())
		})
		t.Run("return error not found if no executable replay found", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, map[string]string{}, description)
			replayReq1 := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateSuccess)

			replayID1, err := replayRepo.RegisterReplay(ctx, replayReq1, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID1)

			replayToExecute, err := replayRepo.GetReplayToExecute(ctx)
			assert.ErrorContains(t, err, "no executable replay request found")
			assert.Nil(t, replayToExecute)
		})
	})

	t.Run("GetReplayRequestsByStatus", func(t *testing.T) {
		t.Run("return replay requests given list of status", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
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

			replayReqs, err := replayRepo.GetReplayRequestsByStatus(ctx, []scheduler.ReplayState{scheduler.ReplayStateCreated, scheduler.ReplayStateInProgress})
			assert.Nil(t, err)
			assert.EqualValues(t, []string{jobAName, jobBName}, []string{replayReqs[0].JobName().String(), replayReqs[1].JobName().String()})
		})
	})

	t.Run("GetReplaysByProject", func(t *testing.T) {
		t.Run("return replay list for corresponding project name", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)
			tnnt, _ := tenant.NewTenant("test-project1", "ns-1")
			tnntOther, _ := tenant.NewTenant("test-project2", "ns-1")

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
			replayReq1 := scheduler.NewReplayRequest(jobAName, tnnt, replayConfig, scheduler.ReplayStateInProgress)
			replayReq2 := scheduler.NewReplayRequest(jobBName, tnnt, replayConfig, scheduler.ReplayStateCreated)
			replayReq3 := scheduler.NewReplayRequest("sample-job-C", tnntOther, replayConfig, scheduler.ReplayStateFailed)

			replayID1, err := replayRepo.RegisterReplay(ctx, replayReq1, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID1)

			replayID2, err := replayRepo.RegisterReplay(ctx, replayReq2, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID2)

			replayID3, err := replayRepo.RegisterReplay(ctx, replayReq3, jobRunsAllPending)
			assert.Nil(t, err)
			assert.NotNil(t, replayID3)

			replayReqs, err := replayRepo.GetReplaysByProject(ctx, tnnt.ProjectName(), 3)
			assert.Nil(t, err)
			assert.Len(t, replayReqs, 2)

			replayReqs, err = replayRepo.GetReplaysByProject(ctx, tnntOther.ProjectName(), 3)
			assert.Nil(t, err)
			assert.Len(t, replayReqs, 1)
		})
		t.Run("return empty list when replay is not existed on given project", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)
			replayReqs, err := replayRepo.GetReplaysByProject(ctx, tnnt.ProjectName(), 3)
			assert.Nil(t, err)
			assert.Len(t, replayReqs, 0)
		})
	})

	t.Run("GetReplayJobConfig", func(t *testing.T) {
		t.Run("return replay task config when scheduledAt is provided", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)
			startTime, _ := time.Parse(scheduler.ISODateFormat, "2022-01-01T15:04:05Z")
			endTime, _ := time.Parse(scheduler.ISODateFormat, "2022-01-03T15:04:05Z")
			scheduledAt, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, map[string]string{"EXECUTION_PROJECT": "example1"}, description)
			replayReq := scheduler.NewReplayRequest(jobBName, tnnt, replayConfig, scheduler.ReplayStateCreated)
			_, err := replayRepo.RegisterReplay(ctx, replayReq, jobRunsAllPending)
			assert.Nil(t, err)
			replayConfig = scheduler.NewReplayConfig(startTime, endTime, true, replayJobConfig, description)
			replayReq = scheduler.NewReplayRequest(jobBName, tnnt, replayConfig, scheduler.ReplayStateCreated)
			_, err = replayRepo.RegisterReplay(ctx, replayReq, jobRunsAllPending)
			assert.Nil(t, err)

			actualReplayJobConfig, err := replayRepo.GetReplayJobConfig(ctx, tnnt, jobBName, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, replayJobConfig, actualReplayJobConfig)
		})
		t.Run("return empty replay task config when there's no extra config in replay config", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)
			startTime, _ := time.Parse(scheduler.ISODateFormat, "2022-01-01T15:04:05Z")
			endTime, _ := time.Parse(scheduler.ISODateFormat, "2022-01-03T15:04:05Z")
			scheduledAt, _ := time.Parse(scheduler.ISODateFormat, "2022-01-02T15:04:05Z")

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, map[string]string{}, description)
			replayReq := scheduler.NewReplayRequest(jobBName, tnnt, replayConfig, scheduler.ReplayStateCreated)
			_, err := replayRepo.RegisterReplay(ctx, replayReq, jobRunsAllPending)
			assert.Nil(t, err)

			actualReplayJobConfig, err := replayRepo.GetReplayJobConfig(ctx, tnnt, jobBName, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, map[string]string{}, actualReplayJobConfig)
		})
	})
}
