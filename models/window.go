package models

import (
	"time"
)

type Window interface {
	Validate() error

	GetStartTime(scheduleTime time.Time) (time.Time, error)
	GetEndTime(scheduleTime time.Time) (time.Time, error)

	GetTruncateTo() string

	GetOffsetAsDuration() time.Duration
	GetOffset() string

	GetSizeAsDuration() time.Duration
	GetSize() string
}

/*
approach 1
	one struct for version 1
approach 2
	two structs for version 1
*/
