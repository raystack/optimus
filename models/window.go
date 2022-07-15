package models

import (
	"fmt"
	"time"
)

type Window interface {
	Validate() error
	GetTimeRange(scheduleTime time.Time) (time.Time, time.Time, error)

	GetTruncateTo() string
	GetOffset() string
	GetSize() string
}

func NewWindow(version int, truncateTo, offset, size string) (Window, error) {
	if version == 1 {
		return windowV1{size, offset, truncateTo}, nil
	}
	if version == 2 {
		return windowV2{size, offset, truncateTo}, nil
	}
	return nil, fmt.Errorf("window version [%d] is not recognized", version)
}
