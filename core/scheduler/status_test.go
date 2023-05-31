package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/lib/cron"
)

func TestStatus(t *testing.T) {
	t.Run("JobRunStatusFrom", func(t *testing.T) {
		currentTime := time.Now()
		expectedJobRunStatus, err := scheduler.JobRunStatusFrom(currentTime, "pending")
		assert.Nil(t, err)
		assert.Equal(t, scheduler.JobRunStatus{
			ScheduledAt: currentTime,
			State:       scheduler.StatePending,
		}, expectedJobRunStatus)

		expectedJobRunStatus, err = scheduler.JobRunStatusFrom(currentTime, "unregisteredState")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity jobRun: invalid state for run unregisteredState")
		assert.Equal(t, scheduler.JobRunStatus{}, expectedJobRunStatus)
	})
	t.Run("ExecutionStart", func(t *testing.T) {
		startDate := time.Date(2022, 1, 1, 1, 1, 1, 0, time.UTC)
		criteria := scheduler.JobRunsCriteria{
			Name:        "JobName",
			StartDate:   startDate,
			EndDate:     startDate.Add(time.Hour),
			Filter:      nil,
			OnlyLastRun: false,
		}
		schedule, err := cron.ParseCronSchedule("@midnight")
		assert.Nil(t, err)
		resp := criteria.ExecutionStart(schedule)
		expectedDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, expectedDate, resp)
	})
	t.Run("ExecutionEndDate", func(t *testing.T) {
		t.Run("when the current time matches one of the schedule times execution time means previous schedule", func(t *testing.T) {
			endDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
			criteria := scheduler.JobRunsCriteria{
				Name:        "JobName",
				StartDate:   endDate.Add(-time.Hour),
				EndDate:     endDate,
				Filter:      nil,
				OnlyLastRun: false,
			}
			schedule, err := cron.ParseCronSchedule("@midnight")
			assert.Nil(t, err)
			resp := criteria.ExecutionEndDate(schedule)
			expectedDate := time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC)
			assert.Equal(t, expectedDate, resp)
		})

		startDate := time.Date(2022, 1, 1, 1, 1, 1, 0, time.UTC)
		criteria := scheduler.JobRunsCriteria{
			Name:        "JobName",
			StartDate:   startDate,
			EndDate:     startDate.Add(time.Hour),
			Filter:      nil,
			OnlyLastRun: false,
		}
		schedule, err := cron.ParseCronSchedule("@midnight")
		assert.Nil(t, err)
		resp := criteria.ExecutionEndDate(schedule)
		expectedDate := time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, expectedDate, resp)
	})
	t.Run("State to string", func(t *testing.T) {
		expectationsMap := map[string]scheduler.State{
			"pending":  scheduler.StatePending,
			"accepted": scheduler.StateAccepted,
			"running":  scheduler.StateRunning,
			"queued":   scheduler.StateQueued,
			"success":  scheduler.StateSuccess,
			"failed":   scheduler.StateFailed,
		}
		for expectedString, input := range expectationsMap {
			assert.Equal(t, expectedString, input.String())
		}
	})
	t.Run("StateFromString", func(t *testing.T) {
		expectationsMap := map[string]scheduler.State{
			"pending":  scheduler.StatePending,
			"PENDING":  scheduler.StatePending,
			"accepted": scheduler.StateAccepted,
			"ACCEPTED": scheduler.StateAccepted,
			"running":  scheduler.StateRunning,
			"RUNNING":  scheduler.StateRunning,
			"queued":   scheduler.StateQueued,
			"QUEUED":   scheduler.StateQueued,
			"success":  scheduler.StateSuccess,
			"SUCCESS":  scheduler.StateSuccess,
			"failed":   scheduler.StateFailed,
			"FAILED":   scheduler.StateFailed,
			"replayed": scheduler.StateReplayed,
			"REPLAYED": scheduler.StateReplayed,
		}
		for input, expectedState := range expectationsMap {
			respState, err := scheduler.StateFromString(input)
			assert.Nil(t, err)
			assert.Equal(t, expectedState, respState)
		}

		respState, err := scheduler.StateFromString("unregisteredState")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity jobRun: invalid state for run unregisteredState")
		assert.Equal(t, scheduler.State(""), respState)
	})
	t.Run("GetLogicalTime", func(t *testing.T) {
		time1 := time.Date(2023, 0o1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 0o1, 2, 0, 0, 0, 0, time.UTC)
		schedule, err := cron.ParseCronSchedule("@midnight")
		assert.Nil(t, err)

		jobRunStatus, err := scheduler.JobRunStatusFrom(time2, "running")
		assert.Nil(t, err)

		logicalTime := jobRunStatus.GetLogicalTime(schedule)
		assert.Equal(t, time1, logicalTime)
	})
	t.Run("GetSortedRunsByStates", func(t *testing.T) {
		time1 := time.Date(2023, 0o1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 0o1, 2, 0, 0, 0, 0, time.UTC)
		time3 := time.Date(2023, 0o1, 3, 0, 0, 0, 0, time.UTC)

		jobRunStatusList := scheduler.JobRunStatusList([]*scheduler.JobRunStatus{
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
			{
				ScheduledAt: time1,
				State:       scheduler.StatePending,
			},
			{
				ScheduledAt: time2,
				State:       scheduler.StateRunning,
			},
		})
		expectedRuns := []*scheduler.JobRunStatus{
			{
				ScheduledAt: time2,
				State:       scheduler.StateRunning,
			},
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
		}

		runs := jobRunStatusList.GetSortedRunsByStates([]scheduler.State{scheduler.StateRunning})
		assert.Equal(t, expectedRuns, runs)
	})
	t.Run("GetSortedRunsByScheduledAt", func(t *testing.T) {
		time1 := time.Date(2023, 0o1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 0o1, 2, 0, 0, 0, 0, time.UTC)
		time3 := time.Date(2023, 0o1, 3, 0, 0, 0, 0, time.UTC)

		jobRunStatusList := scheduler.JobRunStatusList([]*scheduler.JobRunStatus{
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
			{
				ScheduledAt: time1,
				State:       scheduler.StatePending,
			},
			{
				ScheduledAt: time2,
				State:       scheduler.StateRunning,
			},
		})
		expectedRuns := []*scheduler.JobRunStatus{
			{
				ScheduledAt: time1,
				State:       scheduler.StatePending,
			},
			{
				ScheduledAt: time2,
				State:       scheduler.StateRunning,
			},
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
		}

		runs := jobRunStatusList.GetSortedRunsByScheduledAt()
		assert.Equal(t, expectedRuns, runs)
	})
	t.Run("MergeWithUpdatedRuns", func(t *testing.T) {
		time1 := time.Date(2023, 0o1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 0o1, 2, 0, 0, 0, 0, time.UTC)
		time3 := time.Date(2023, 0o1, 3, 0, 0, 0, 0, time.UTC)

		jobRunStatusList := scheduler.JobRunStatusList([]*scheduler.JobRunStatus{
			{
				ScheduledAt: time1,
				State:       scheduler.StatePending,
			},
			{
				ScheduledAt: time2,
				State:       scheduler.StateRunning,
			},
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
		})
		updatedRuns := map[time.Time]scheduler.State{
			time1: scheduler.StateSuccess,
			time2: scheduler.StateSuccess,
		}
		expectedRuns := []*scheduler.JobRunStatus{
			{
				ScheduledAt: time1,
				State:       scheduler.StateSuccess,
			},
			{
				ScheduledAt: time2,
				State:       scheduler.StateSuccess,
			},
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
		}

		mergedRuns := jobRunStatusList.MergeWithUpdatedRuns(updatedRuns)

		assert.Equal(t, expectedRuns, mergedRuns)
	})
	t.Run("ToRunStatusMap", func(t *testing.T) {
		time1 := time.Date(2023, 0o1, 1, 0, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 0o1, 2, 0, 0, 0, 0, time.UTC)
		time3 := time.Date(2023, 0o1, 3, 0, 0, 0, 0, time.UTC)

		jobRunStatusList := scheduler.JobRunStatusList([]*scheduler.JobRunStatus{
			{
				ScheduledAt: time1,
				State:       scheduler.StatePending,
			},
			{
				ScheduledAt: time2,
				State:       scheduler.StateRunning,
			},
			{
				ScheduledAt: time3,
				State:       scheduler.StateRunning,
			},
		})
		expectedMap := map[time.Time]scheduler.State{
			time1: scheduler.StatePending,
			time2: scheduler.StateRunning,
			time3: scheduler.StateRunning,
		}

		runStatusMap := jobRunStatusList.ToRunStatusMap()
		assert.EqualValues(t, expectedMap, runStatusMap)
	})
}
