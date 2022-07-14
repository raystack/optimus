package job

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/odpf/optimus/utils"
)

type WindowV2 struct {
	Size       string
	Offset     string
	TruncateTo string
}

func (w WindowV2) Validate() error {
	if w.Size != "" {
		err := w.validateSize()
		if err != nil {
			return err
		}
	}
	if w.Offset != "" {
		_, nonMonthDuration, err := w.getMonthsAndDuration(w.Offset)
		if nonMonthDuration != "" {
			_, err = time.ParseDuration(nonMonthDuration)
			if err != nil {
				return fmt.Errorf("failed to parse task window with size %v: %w", w.Offset, err)
			}
		}
	}
	if w.TruncateTo != "" {
		validTruncateOptions := []string{"h", "d", "w", "M"}
		if utils.ContainsString(validTruncateOptions, w.TruncateTo) == false {
			return fmt.Errorf("invalid option provided, provide one of : %v", validTruncateOptions)
		}
	}
	return nil
}

func (w WindowV2) validateSize() error {
	months, nonMonthDuration, err := w.getMonthsAndDuration(w.Size)
	if err != nil {
		return err
	}
	if months < 0 {
		return fmt.Errorf("size can't be negative %s", w.Size)
	}
	if nonMonthDuration != "" {
		_, err = time.ParseDuration(nonMonthDuration)
		if err != nil {
			return fmt.Errorf("failed to parse task window with size %v: %w", w.Size, err)
		}
	}
	if strings.HasPrefix(w.Size, "-") {
		return fmt.Errorf("size cannot be negative, %s", w.Size)
	}
	return nil
}

func (w WindowV2) getMonthsAndDuration(timeDuration string) (int, string, error) {
	if strings.Contains(timeDuration, "M") == false {
		return 0, timeDuration, nil
	}
	splits := strings.SplitN(timeDuration, "M", 2)
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

func (w WindowV2) GetTimeRange(scheduleTime time.Time) (time.Time, time.Time, error) {
	err := w.Validate()
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	truncatedTime := w.truncateTime(scheduleTime)
	endTime, err := w.adjustOffset(truncatedTime)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	startTime, err := w.getStartTime(endTime)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return startTime, endTime, nil
}

func (w WindowV2) truncateTime(scheduleTime time.Time) time.Time {
	if w.TruncateTo == "" {
		return scheduleTime
	}
	if w.TruncateTo == "h" {
		// remove time upto hours
		return scheduleTime.Truncate(time.Hour)
	}
	if w.TruncateTo == "d" {
		// remove time upto day
		return scheduleTime.Truncate(24 * time.Hour)
	}
	if w.TruncateTo == "w" {
		truncatedToDay := scheduleTime.Truncate(24 * time.Hour)
		// weekday with start of the week as Monday
		weekday := scheduleTime.Weekday()
		if weekday == 0 {
			weekday = 7
		}
		daysToSubtract := weekday - time.Monday

		durationToSubtract := time.Duration(daysToSubtract) * 24 * time.Hour
		return truncatedToDay.Add(-durationToSubtract)
	}
	if w.TruncateTo == "M" {
		return time.Date(scheduleTime.Year(), scheduleTime.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return scheduleTime
}

func (w WindowV2) adjustOffset(truncatedTime time.Time) (time.Time, error) {
	if w.Offset == "" {
		return truncatedTime, nil
	}
	months, nonMonthDurationString, err := w.getMonthsAndDuration(w.Offset)
	if err != nil {
		return time.Time{}, err
	}

	nonMonthDuration, err := time.ParseDuration(nonMonthDurationString)
	if err != nil {
		return time.Time{}, err
	}
	return truncatedTime.Add(nonMonthDuration).AddDate(0, months, 0), nil
}

func (w WindowV2) getStartTime(endTime time.Time) (time.Time, error) {
	if w.Size == "" {
		return endTime, nil
	}
	months, nonMonthDurationString, err := w.getMonthsAndDuration(w.Size)
	if err != nil {
		return time.Time{}, err
	}
	nonMonthDuration, err := time.ParseDuration(nonMonthDurationString)
	if err != nil { // not expecting this, if this happens due to bad code just return inputTime
		return time.Time{}, err
	}
	return endTime.Add(-nonMonthDuration).AddDate(0, -months, 0), nil
}
