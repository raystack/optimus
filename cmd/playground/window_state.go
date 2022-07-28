package playground

import (
	"time"

	"github.com/odpf/optimus/job"
)

type state struct {
	windowV1      job.WindowV1
	windowV2      job.WindowV2
	scheduledTime time.Time
}

func (s *state) incrementScheduleTimeOn(cursor cursorPointer) {
	switch cursor {
	case pointToMinute:
		s.scheduledTime = s.scheduledTime.Add(time.Minute)
	case pointToHour:
		s.scheduledTime = s.scheduledTime.Add(time.Hour)
	case PointToDay:
		s.scheduledTime = s.scheduledTime.AddDate(0, 0, 1)
	case pointToMonth:
		s.scheduledTime = s.scheduledTime.AddDate(0, 1, 0)
	case pointToYear:
		s.scheduledTime = s.scheduledTime.AddDate(1, 0, 0)
	}
}

func (s *state) decrementScheduleTimeOn(cursor cursorPointer) {
	switch cursor {
	case pointToMinute:
		s.scheduledTime = s.scheduledTime.Add(-1 * time.Minute)
	case pointToHour:
		s.scheduledTime = s.scheduledTime.Add(-1 * time.Hour)
	case PointToDay:
		s.scheduledTime = s.scheduledTime.AddDate(0, 0, -1)
	case pointToMonth:
		s.scheduledTime = s.scheduledTime.AddDate(0, -1, 0)
	case pointToYear:
		s.scheduledTime = s.scheduledTime.AddDate(-1, 0, 0)
	}
}

func (s *state) incrementTruncateTo() {
	switch s.windowV2.TruncateTo {
	case "w":
		s.windowV2.TruncateTo = "M"
	case "d":
		s.windowV2.TruncateTo = "w"
	case "h":
		s.windowV2.TruncateTo = "d"
	}
}

func (s *state) decrementTruncateTo() {
	switch s.windowV2.TruncateTo {
	case "d":
		s.windowV2.TruncateTo = "h"
	case "w":
		s.windowV2.TruncateTo = "d"
	case "M":
		s.windowV2.TruncateTo = "w"
	}
}
func (s *state) updateWindowParameters(size string, offset string) {
	s.windowV1.Size = size
	s.windowV2.Size = size
	s.windowV1.Offset = offset
	s.windowV2.Offset = offset
	s.windowV1.TruncateTo = s.windowV2.TruncateTo
}
