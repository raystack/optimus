package scheduler_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
)

func TestReplay(t *testing.T) {
	replayID := uuid.New()
	jobNameA, _ := scheduler.JobNameFrom("sample-job-A")
	projName := tenant.ProjectName("proj")
	namespaceName := tenant.ProjectName("ns1")
	tnnt, _ := tenant.NewTenant(projName.String(), namespaceName.String())
	startTimeStr := "2023-01-02T00:00:00Z"
	startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
	endTime := startTime.Add(48 * time.Hour)
	replayDescription := "sample backfill"
	replayConfig := scheduler.NewReplayConfig(startTime, endTime, false, replayDescription)
	scheduledTimeStr1 := "2023-01-02T12:00:00Z"
	scheduledTime1, _ := time.Parse(scheduler.ISODateFormat, scheduledTimeStr1)
	scheduledTime2 := scheduledTime1.Add(24 * time.Hour)

	t.Run("NewReplay", func(t *testing.T) {
		createdTime := time.Now()
		replay := scheduler.NewReplay(replayID, jobNameA, tnnt, replayConfig, scheduler.ReplayStateCreated, createdTime)

		assert.Equal(t, replayID, replay.ID())
		assert.Equal(t, jobNameA, replay.JobName())
		assert.Equal(t, tnnt, replay.Tenant())
		assert.Equal(t, replayConfig, replay.Config())
		assert.Equal(t, scheduler.ReplayStateCreated.String(), replay.State().String())
		assert.Equal(t, "", replay.Message())
		assert.Equal(t, createdTime, replay.CreatedAt())
	})

	t.Run("NewReplayRequest", func(t *testing.T) {
		replay := scheduler.NewReplayRequest(jobNameA, tnnt, replayConfig, scheduler.ReplayStateCreated)

		assert.Equal(t, uuid.Nil, replay.ID())
		assert.Equal(t, jobNameA, replay.JobName())
		assert.Equal(t, tnnt, replay.Tenant())
		assert.Equal(t, replayConfig, replay.Config())
		assert.Equal(t, scheduler.ReplayStateCreated.String(), replay.State().String())
		assert.Equal(t, "", replay.Message())
	})

	t.Run("ReplayWithRun", func(t *testing.T) {
		firstRun := &scheduler.JobRunStatus{
			ScheduledAt: scheduledTime1,
			State:       scheduler.StatePending,
		}
		secondRun := &scheduler.JobRunStatus{
			ScheduledAt: scheduledTime2,
			State:       scheduler.StatePending,
		}

		t.Run("GetFirstExecutableRun", func(t *testing.T) {
			replay := scheduler.NewReplay(replayID, jobNameA, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now())
			replayWithRun := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs: []*scheduler.JobRunStatus{
					firstRun,
					secondRun,
				},
			}
			firstExecutableRun := replayWithRun.GetFirstExecutableRun()
			assert.Equal(t, firstRun, firstExecutableRun)
		})
		t.Run("GetLastExecutableRun", func(t *testing.T) {
			replay := scheduler.NewReplay(replayID, jobNameA, tnnt, replayConfig, scheduler.ReplayStateCreated, time.Now())
			replayWithRun := &scheduler.ReplayWithRun{
				Replay: replay,
				Runs: []*scheduler.JobRunStatus{
					firstRun,
					secondRun,
				},
			}
			lastExecutableRun := replayWithRun.GetLastExecutableRun()
			assert.Equal(t, secondRun, lastExecutableRun)
		})
	})

	t.Run("ReplayStateFromString", func(t *testing.T) {
		expectationsMap := map[string]scheduler.ReplayState{
			"created":          scheduler.ReplayStateCreated,
			"CREATED":          scheduler.ReplayStateCreated,
			"in progress":      scheduler.ReplayStateInProgress,
			"IN PROGRESS":      scheduler.ReplayStateInProgress,
			"invalid":          scheduler.ReplayStateInvalid,
			"INVALID":          scheduler.ReplayStateInvalid,
			"partial replayed": scheduler.ReplayStatePartialReplayed,
			"PARTIAL REPLAYED": scheduler.ReplayStatePartialReplayed,
			"replayed":         scheduler.ReplayStateReplayed,
			"REPLAYED":         scheduler.ReplayStateReplayed,
			"success":          scheduler.ReplayStateSuccess,
			"SUCCESS":          scheduler.ReplayStateSuccess,
			"failed":           scheduler.ReplayStateFailed,
			"FAILED":           scheduler.ReplayStateFailed,
		}
		for input, expectedState := range expectationsMap {
			respState, err := scheduler.ReplayStateFromString(input)
			assert.Nil(t, err)
			assert.Equal(t, expectedState, respState)
		}

		respState, err := scheduler.ReplayStateFromString("unregisteredState")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity jobRun: invalid state for replay unregisteredState")
		assert.Equal(t, scheduler.ReplayState(""), respState)
	})
}
