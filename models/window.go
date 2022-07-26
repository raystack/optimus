package models

import (
	"fmt"
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

func NewWindow(version int, truncateTo, offset, size string) (Window, error) {
	if version == 1 {
		return windowV1{truncateTo: truncateTo, offset: offset, size: size}, nil
	}
	if version == 2 { // nolint:gomnd
		return windowV2{truncateTo: truncateTo, offset: offset, size: size}, nil
	}
	return nil, fmt.Errorf("window version [%d] is not recognized", version)
}
