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
}
