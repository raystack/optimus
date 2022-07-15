package models

import (
	"fmt"
	"time"
)

type Window interface {
	GetTimeRange(scheduleTime time.Time) (time.Time, time.Time, error)
	Validate() error
}

func NewWindow(version int, truncateTo, offset, size string) (Window, error) {
	if version == 1 {
		return WindowV1{size, offset, truncateTo}, nil
	}
	if version == 2 {
		return WindowV2{size, offset, truncateTo}, nil
	}
	return nil, fmt.Errorf("window version [%d] is not recognized", version)
}
