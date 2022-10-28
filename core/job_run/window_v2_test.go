package job_run_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job_run"
)

func TestWindowV2(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("should not throw error for window size which is not a positive or an instant time duration", func(t *testing.T) {
			validWindowConfigs := []string{"24h", "2h45m", "60s", "45m24h", "", "0", "2M", "45M24h", "45M24h30m"}
			for _, config := range validWindowConfigs {
				window, err := job_run.NewWindow(2, "", "", config)
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should throw error for window size which is not a valid time duration", func(t *testing.T) {
			inValidWindowConfigs := []string{"60S", "60", "2d", "-24h", "-45M24h30m"}
			for _, config := range inValidWindowConfigs {
				window, err := job_run.NewWindow(2, "", "", config)
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.NotNil(t, err, fmt.Sprintf("failed for %s", config))
			}
		})
		t.Run("should not throw error for window offset which is not a time duration", func(t *testing.T) {
			validOffsetConfigs := []string{"24h", "2h45m", "60s", "45m24h", "0", "", "2M", "45M24h", "45M24h30m", "-45M24h30m"}
			for _, config := range validOffsetConfigs {
				window, err := job_run.NewWindow(2, "", config, "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should throw error for window offset which is not a valid time duration", func(t *testing.T) {
			inValidOffsetConfigs := []string{"60S", "60"}
			for _, config := range inValidOffsetConfigs {
				window, err := job_run.NewWindow(2, "", config, "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.NotNil(t, err, fmt.Sprintf("failed for %s", config))
			}
		})
		t.Run("should not throw error for valid window truncate configs", func(t *testing.T) {
			validTruncateConfigs := []string{"h", "d", "w", "M", ""}
			for _, config := range validTruncateConfigs {
				window, err := job_run.NewWindow(2, config, "", "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should throw error for window truncate when it is not a truncate option", func(t *testing.T) {
			inValidTruncateConfigs := []string{"s", "a", "ms", "m", "H", "D", "W"}
			for _, config := range inValidTruncateConfigs {
				window, err := job_run.NewWindow(2, config, "", "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.NotNil(t, err, fmt.Sprintf("failed for %s", config))
			}
		})
	})
	t.Run("GetTimeRange", func(t *testing.T) {
		t.Run("should provide start and end time window for the given schedule", func(t *testing.T) {
			cases := []struct {
				Scenario          string
				ScheduleTime      time.Time
				Size              string
				Offset            string
				TruncateTo        string
				ExpectedStartTime time.Time
				ExpectedEndTime   time.Time
			}{
				{
					Scenario:          "should truncate to the previous hour on truncate to hour",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "h",
					ExpectedStartTime: time.Date(2022, 07, 04, 02, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 02, 0, 0, 0, time.UTC),
				}, {
					Scenario:          "should not truncate if truncate to is empty",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "",
					ExpectedStartTime: time.Date(2022, 07, 04, 02, 10, 10, 10, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
				},
				{
					Scenario:          "should truncate to the previous day on truncate to day",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 04, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should truncate to the start of week starting on monday on truncate to week",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "w",
					ExpectedStartTime: time.Date(2022, 07, 03, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 04, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should truncate to the start of the month on truncate to month",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2022, 06, 30, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 01, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not truncate to the previous hour if time is already truncated",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 00, 00, 00, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "h",
					ExpectedStartTime: time.Date(2022, 07, 04, 02, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 02, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not truncate to the previous day if time is already truncated",
					ScheduleTime:      time.Date(2022, 07, 05, 00, 0, 0, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 04, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not truncate to the start of last week if the time is already on monday on truncate to week",
					ScheduleTime:      time.Date(2022, 07, 04, 00, 0, 0, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "w",
					ExpectedStartTime: time.Date(2022, 07, 03, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 04, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not truncate to the start of month if time is already on beginning of month on truncate to month",
					ScheduleTime:      time.Date(2022, 07, 01, 0, 0, 0, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2022, 06, 30, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 01, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not truncate if truncate is empty",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "",
					ExpectedStartTime: time.Date(2022, 07, 04, 02, 10, 10, 10, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
				},
				{
					Scenario:          "should provide window for the configured size in hours and minutes",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h30m",
					Offset:            "0",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 03, 23, 30, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should provide window for the configured size in months",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "2M",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2022, 5, 01, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 7, 01, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should provide window for the configured size in months, hours",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "1M24h",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2022, 05, 30, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 01, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should provide window for the configured size in minutes",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "60m",
					Offset:            "0",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 04, 23, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not provide any window is size is not configured",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "",
					Offset:            "0",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should shift window forward on positive offset",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "24h",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 06, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should shift window backward on negative monthly offset",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "1M",
					Offset:            "-1M",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2022, 05, 01, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 06, 01, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should shift window backward on negative monthly offset with hours",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "1M",
					Offset:            "-1M24h",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2022, 04, 30, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 05, 30, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should shift window backward on negative offset",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "-24h",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 03, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 04, 0, 0, 0, 0, time.UTC),
				},
				{
					Scenario:          "should not shift window when offset is not configured",
					ScheduleTime:      time.Date(2022, 07, 05, 02, 10, 10, 10, time.UTC),
					Size:              "24h",
					Offset:            "",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2022, 07, 04, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2022, 07, 05, 0, 0, 0, 0, time.UTC),
				},
			}
			for _, sc := range cases {
				w, err := job_run.NewWindow(2, sc.TruncateTo, sc.Offset, sc.Size)
				if err != nil {
					panic(err)
				}

				actualValidateError := w.Validate()
				actualStartTime, actualStartTimeError := w.GetStartTime(sc.ScheduleTime)
				actualEndTime, actualEndTimeError := w.GetEndTime(sc.ScheduleTime)

				assert.NoError(t, actualValidateError, sc.Scenario)
				assert.Equal(t, sc.ExpectedStartTime.String(), actualStartTime.String(), sc.Scenario)
				assert.NoError(t, actualStartTimeError, sc.Scenario)
				assert.Equal(t, sc.ExpectedEndTime.String(), actualEndTime.String(), sc.Scenario)
				assert.NoError(t, actualEndTimeError, sc.Scenario)
			}
		})
	})
}
