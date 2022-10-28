package job_run

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityWindow = "window"
)

type Window interface {
	Validate() error

	GetStartTime(scheduleTime time.Time) (time.Time, error)
	GetEndTime(scheduleTime time.Time) (time.Time, error)
	GetTruncateTo() string
	GetOffset() string
	GetSize() string
	GetVersion() int
}

func NewWindow(version int, truncateTo, offset, size string) (Window, error) {
	if version == 1 {
		return windowV1{truncateTo: truncateTo, offset: offset, size: size}, nil
	}
	if version == 2 { // nolint:gomnd
		return windowV2{truncateTo: truncateTo, offset: offset, size: size}, nil
	}
	return nil, errors.InvalidArgument(EntityWindow, fmt.Sprintf("window version [%d] is not recognized", version))
}

// GetEndRunDate subtract 1 day to make end inclusive
func GetEndRunDate(runTime time.Time, window Window) (time.Time, error) {
	months, nonMonthDurationString, err := monthsAndNonMonthExpression(window.GetSize())
	if err != nil {
		return time.Time{}, err
	}

	nonMonthDuration, err := time.ParseDuration(nonMonthDurationString)
	if err != nil {
		return time.Time{}, err
	}
	return runTime.Add(nonMonthDuration).AddDate(0, months, 0).Add(time.Hour * -24), nil
}

func monthsAndNonMonthExpression(durationExpression string) (int, string, error) {
	if strings.Contains(durationExpression, "M") == false {
		return 0, durationExpression, nil
	}
	maxSubString := 2
	splits := strings.SplitN(durationExpression, "M", maxSubString)
	months, err := strconv.Atoi(splits[0])
	if err != nil {
		return 0, "0", err
	}
	// duration contains only month
	if len(splits) == 1 || splits[1] == "" {
		return months, "0", nil
	}
	// if duration is negative then use the negative duration for both the splits.
	if months < 0 {
		return months, "-" + splits[1], nil
	}
	return months, splits[1], nil
}
