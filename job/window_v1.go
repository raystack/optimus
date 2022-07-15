package job

import (
	"fmt"
	"strings"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

type WindowV1 struct {
	Size       string
	Offset     string
	TruncateTo string
}

func (w WindowV1) Validate() error {
	if w.Size != "" {
		_, err := time.ParseDuration(w.Size)
		if err != nil {
			return fmt.Errorf("failed to parse task window with size %v: %w", w.Size, err)
		}
		if strings.HasPrefix(w.Size, "-") {
			return fmt.Errorf("size cannot be negative, %s", w.Size)
		}
	}
	if w.Offset != "" {
		_, err := time.ParseDuration(w.Offset)
		if err != nil {
			return fmt.Errorf("failed to parse task window with size %v: %w", w.Offset, err)
		}
	}
	if w.TruncateTo != "" {
		validTruncateOptions := []string{"h", "d", "w", "m", "M"}
		if utils.ContainsString(validTruncateOptions, w.TruncateTo) == false {
			return fmt.Errorf("invalid option provided, provide one of : %v", validTruncateOptions)
		}
	}
	return nil
}

func (w WindowV1) GetTimeRange(scheduleTime time.Time) (time.Time, time.Time, error) {
	var err error
	err = w.Validate()
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	jobWindow, err := prepareWindow(w.Size, w.Offset, w.TruncateTo)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	startTime := jobWindow.GetStart(scheduleTime)
	endTime := jobWindow.GetEnd(scheduleTime)
	return startTime, endTime, nil
}

const HoursInDay = time.Hour * 24

func prepareWindow(windowSize, windowOffset, truncateTo string) (models.JobSpecTaskWindow, error) {
	var err error
	window := models.JobSpecTaskWindow{}
	window.Size = HoursInDay
	window.Offset = 0
	window.TruncateTo = "d"

	if truncateTo != "" {
		window.TruncateTo = truncateTo
	}
	if windowSize != "" {
		window.Size, err = time.ParseDuration(windowSize)
		if err != nil {
			return window, fmt.Errorf("failed to parse task window with size %v: %w", windowSize, err)
		}
	}
	if windowOffset != "" {
		window.Offset, err = time.ParseDuration(windowOffset)
		if err != nil {
			return window, fmt.Errorf("failed to parse task window with offset %v: %w", windowOffset, err)
		}
	}
	return window, nil
}
