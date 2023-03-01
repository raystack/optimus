package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/lib/cron"
)

func TestStatus(t *testing.T) {
	t.Run("JobRunStatusFrom", func(t *testing.T) {
		currentTime := time.Now()
		logicalTime := currentTime.Add(24 * time.Hour)
		expectedJobRunStatus, err := scheduler.JobRunStatusFrom(currentTime, "pending", logicalTime)
		assert.Nil(t, err)
		assert.Equal(t, scheduler.JobRunStatus{
			ScheduledAt: currentTime,
			State:       scheduler.StatePending,
			LogicalTime: logicalTime,
		}, expectedJobRunStatus)

		expectedJobRunStatus, err = scheduler.JobRunStatusFrom(currentTime, "unregisteredState", logicalTime)
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
			"failed":   scheduler.StateFailed}
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
}
