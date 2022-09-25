package models_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
)

func TestWindowV1(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("should not return error for window size which is not a positive or an instant time duration", func(t *testing.T) {
			validWindowConfigs := []string{"24h", "2h45m", "60s", "45m24h", "", "0"}
			for _, config := range validWindowConfigs {
				window, err := models.NewWindow(1, "", "", config)
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should not return error for window offset which is a valid time duration", func(t *testing.T) {
			validOffsetConfigs := []string{"24h", "2h45m", "60s", "45m24h", "0", ""}
			for _, config := range validOffsetConfigs {
				window, err := models.NewWindow(1, "", config, "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should not return error for valid window truncate configs", func(t *testing.T) {
			validTruncateConfigs := []string{"h", "d", "w", "M", ""}
			for _, config := range validTruncateConfigs {
				window, err := models.NewWindow(1, config, "", "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
	})
	t.Run("GetTimeRange", func(t *testing.T) {
		t.Run("should provide start and end time window for the given schedule", func(t *testing.T) {
			cases := []struct {
				ScheduleTime      time.Time
				Size              string
				Offset            string
				TruncateTo        string
				ExpectedStartTime time.Time
				ExpectedEndTime   time.Time
			}{
				{
					ScheduleTime:      time.Date(2021, 2, 25, 0, 0, 0, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "",
					ExpectedStartTime: time.Date(2021, 2, 24, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 2, 25, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "",
					ExpectedStartTime: time.Date(2020, 7, 9, 6, 33, 22, 0, time.UTC),  // modified from the original, since it was not consistent with the implementation default truncate. original [time.Date(2020, 7, 9, 6, 33, 22, 0, time.UTC)]
					ExpectedEndTime:   time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC), // modified from the original, since it was not consistent with the implementation default truncate. original [time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC)]
				},
				{
					ScheduleTime:      time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "h",
					ExpectedStartTime: time.Date(2020, 7, 9, 6, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2020, 7, 10, 6, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					Size:              "24h",
					Offset:            "0",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2020, 7, 10, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					Size:              "48h",
					Offset:            "24h",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2020, 7, 11, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					Size:              "24h",
					Offset:            "-24h",
					TruncateTo:        "d",
					ExpectedStartTime: time.Date(2020, 7, 8, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					Size:              "168h",
					Offset:            "0",
					TruncateTo:        "w",
					ExpectedStartTime: time.Date(2020, 7, 5, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2020, 7, 12, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					Size:              "720h",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					Size:              "720h",
					Offset:            "720h",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 3, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					Size:              "720h",
					Offset:            "-720h",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					Size:              "480h",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					Size:              "1440h",
					Offset:            "0",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					ScheduleTime:      time.Date(2021, 3, 31, 6, 33, 22, 0, time.UTC),
					Size:              "720h",
					Offset:            "-720h",
					TruncateTo:        "M",
					ExpectedStartTime: time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEndTime:   time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
			}
			for _, sc := range cases {
				w, err := models.NewWindow(1, sc.TruncateTo, sc.Offset, sc.Size)
				if err != nil {
					panic(err)
				}

				actualValidateError := w.Validate()
				actualStartTime, actualStartTimeError := w.GetStartTime(sc.ScheduleTime)
				actualEndTime, actualEndTimeError := w.GetEndTime(sc.ScheduleTime)

				assert.NoError(t, actualValidateError)
				assert.Equal(t, sc.ExpectedStartTime.String(), actualStartTime.String())
				assert.NoError(t, actualStartTimeError)
				assert.Equal(t, sc.ExpectedEndTime.String(), actualEndTime.String())
				assert.NoError(t, actualEndTimeError)
			}
		})
	})
}
