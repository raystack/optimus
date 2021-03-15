package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJob(t *testing.T) {
	t.Run("JobSpecTaskWindow", func(t *testing.T) {
		t.Run("should generate valid window start and end", func(t *testing.T) {
			cases := []struct {
				Today              time.Time
				WindowSize         time.Duration
				WindowOffset       time.Duration
				WindowTruncateUpto string

				ExpectedStart time.Time
				ExpectedEnd   time.Time
			}{
				{
					Today:              time.Date(2021, 2, 25, 0, 0, 0, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "",
					ExpectedStart:      time.Date(2021, 2, 24, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 25, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "",
					ExpectedStart:      time.Date(2020, 7, 9, 6, 33, 22, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "h",
					ExpectedStart:      time.Date(2020, 7, 9, 6, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 10, 6, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "d",
					ExpectedStart:      time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 10, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         48 * time.Hour,
					WindowOffset:       24 * time.Hour,
					WindowTruncateUpto: "d",
					ExpectedStart:      time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 11, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       -24 * time.Hour,
					WindowTruncateUpto: "d",
					ExpectedStart:      time.Date(2020, 7, 8, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 7 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "w",
					ExpectedStart:      time.Date(2020, 7, 5, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 12, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       24 * 30 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 3, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       -24 * 30 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 20 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 60 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 3, 31, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       -24 * 30 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
			}

			for _, tcase := range cases {
				win := &JobSpecTaskWindow{
					Size:       tcase.WindowSize,
					Offset:     tcase.WindowOffset,
					TruncateTo: tcase.WindowTruncateUpto,
				}
				windowStart := win.GetStart(tcase.Today)
				windowEnd := win.GetEnd(tcase.Today)
				assert.Equal(t, tcase.ExpectedStart, windowStart)
				assert.Equal(t, tcase.ExpectedEnd, windowEnd)
			}
		})
	})
}
