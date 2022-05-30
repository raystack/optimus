package cron

import (
	"time"

	roboCron "github.com/robfig/cron/v3"
)

type ScheduleSpec struct {
	schd roboCron.Schedule
}

// Next accepts the time and returns the next run time that should
// be used for execution
func (s *ScheduleSpec) Next(t time.Time) time.Time {
	return s.schd.Next(t)
}

// ParseCronSchedule can parse standard cron notation
// it returns a new crontab schedule representing the given
// standardSpec (https://en.wikipedia.org/wiki/Cron). It requires 5 entries
// representing: minute, hour, day of month, month and day of week, in that
// order. It returns a descriptive error if the spec is not valid.
//
// It accepts
//   - Standard crontab specs, e.g. "* * * * ?"
//   - Descriptors, e.g. "@midnight", "@every 1h30m"
func ParseCronSchedule(interval string) (*ScheduleSpec, error) {
	roboCronSchedule, err := roboCron.ParseStandard(interval)
	if err != nil {
		return nil, err
	}

	return &ScheduleSpec{
		schd: roboCronSchedule,
	}, nil
}

// Interval accepts the time and returns duration between
// prev schedule time and current schedule time
func (s *ScheduleSpec) Interval(t time.Time) time.Duration {
	const numberOfIteration = 5

	// Slice to store previous scheduled time from the given parameter time.
	var scheduleArr []time.Time

	// Calculate sum of duration between two job runs.
	var sumDurationBetweenTwoJobs time.Duration
	tempTime := t
	for i := 0; i < numberOfIteration; i++ {
		start := s.Next(tempTime)
		next := s.Next(start)
		sumDurationBetweenTwoJobs += next.Sub(start)
		tempTime = next
	}

	// Average out the duration between two jobs.
	averageDurationBetweenTwoRuns := sumDurationBetweenTwoJobs / numberOfIteration

	// Calculate some point in history from as compare to time in parameter.
	historyStartDate := t.Add(-averageDurationBetweenTwoRuns * numberOfIteration)

	for historyStartDate.Before(t.Add(time.Second * 1)) {
		historyStartDate = s.Next(historyStartDate)
		scheduleArr = append(scheduleArr, historyStartDate)
	}

	// the previous schedule time for parameter t
	var prevStartDate time.Time

	// keep traversing the scheduleArr list till we get the t
	// and then we can get prev schedule based on index.
	for index, scheduleTime := range scheduleArr {
		if scheduleTime.Equal(t) {
			prevStartDate = scheduleArr[index-1]
		}
	}
	duration := t.Sub(prevStartDate)

	return duration
}
