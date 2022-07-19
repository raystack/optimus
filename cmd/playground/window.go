package playground

import (
	"time"

	"github.com/odpf/optimus/job"
)

type state struct {
	windowv1      job.WindowV1
	windowv2      job.WindowV2
	sechduledTime time.Time
}

// increment the component of the time which the cursor is pointed at
func (s *state) IncrementTime(increaseBy string) {
	switch increaseBy {
	case "minute":
		s.sechduledTime = s.sechduledTime.Add(time.Minute)
	case "hour":
		s.sechduledTime = s.sechduledTime.Add(time.Hour)
	case "day":
		s.sechduledTime = s.sechduledTime.AddDate(0, 0, 1)
	case "month":
		s.sechduledTime = s.sechduledTime.AddDate(0, 1, 0)
	case "year":
		s.sechduledTime = s.sechduledTime.AddDate(1, 0, 0)
	}
}

// decrement the component of the time which the cursor is pointed at
func (s *state) DecrementTime(decreaseBy string) {
	switch decreaseBy {
	case "minute":
		s.sechduledTime = s.sechduledTime.Add(-1 * time.Minute)
	case "hour":
		s.sechduledTime = s.sechduledTime.Add(-1 * time.Hour)
	case "day":
		s.sechduledTime = s.sechduledTime.AddDate(0, 0, -1)
	case "month":
		s.sechduledTime = s.sechduledTime.AddDate(0, -1, 0)
	case "year":
		s.sechduledTime = s.sechduledTime.AddDate(-1, 0, 0)
	}
}

// change the value of truncate
func (s *state) IncrementTruncate() string {
	switch s.windowv2.TruncateTo {
	case "h":
		return "d"
	case "d":
		return "w"
	case "w":
		return "M"
	}
	return "h"
}

func (s *state) DecrementTruncate() string {
	switch s.windowv2.TruncateTo {
	case "M":
		return "w"
	case "w":
		return "d"
	case "d":
		return "h"
	case "h":
		return ""
	}
	return "h"
}
