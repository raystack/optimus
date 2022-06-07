package cron_test

import (
	"github.com/odpf/optimus/core/cron"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestScheduleSpec(t *testing.T) {
	t.Run("Prev", func(t *testing.T) {
		t.Run("with constant interval", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@midnight")
			assert.Nil(t, err)
			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
			prevScheduleTime := scheduleSpec.Prev(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-25T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with varying interval", func(t *testing.T) {
			// at 2 AM every month on 2,11,19,26
			scheduleSpec, err := cron.ParseCronSchedule("0 2 2,11,19,26 * *")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-19T01:59:59+00:00")
			prevScheduleTime := scheduleSpec.Prev(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-11T02:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with time falling on schedule time", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@monthly")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-01T00:00:00+00:00")
			prevScheduleTime := scheduleSpec.Prev(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-02-01T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
	})
	t.Run("Next", func(t *testing.T) {
		t.Run("with constant interval", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@midnight")
			assert.Nil(t, err)
			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-25T02:00:00+00:00")
			prevScheduleTime := scheduleSpec.Next(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-26T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with varying interval", func(t *testing.T) {
			// at 2 AM every month on 2,11,19,26
			scheduleSpec, err := cron.ParseCronSchedule("0 2 2,11,19,26 * *")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-19T02:01:59+00:00")
			prevScheduleTime := scheduleSpec.Next(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-03-26T02:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
		t.Run("with current time falling on schedule time", func(t *testing.T) {
			scheduleSpec, err := cron.ParseCronSchedule("@monthly")
			assert.Nil(t, err)

			scheduleStartTime, _ := time.Parse(time.RFC3339, "2022-03-01T00:00:00+00:00")
			prevScheduleTime := scheduleSpec.Next(scheduleStartTime)
			expectedTime, _ := time.Parse(time.RFC3339, "2022-04-01T00:00:00+00:00")
			assert.Equal(t, prevScheduleTime, expectedTime)
		})
	})
}
