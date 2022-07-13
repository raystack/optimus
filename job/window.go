package job

import (
	"fmt"
	"github.com/odpf/optimus/utils"
	"strconv"
	"strings"
	"time"
)

type Window interface {
	GetTimeRange(scheduleTime time.Time) (time.Time, time.Time, error)
	Validate() error
}

func GetWindow(version int, truncateTo, offset, size string) Window {
	if version == 1 {
		return WindowV1{size, offset, truncateTo}
	}
	if version == 2 {
		return WindowV2{size, offset, truncateTo}
	}
	return nil
}

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
		validTruncateOptions := []string{"h", "d", "w", "m", "H", "D", "W", "M"}
		if utils.ContainsString(validTruncateOptions, w.TruncateTo) == false {
			return fmt.Errorf("invalid option provided, provide one of : %v", validTruncateOptions)
		}
	}
	return nil
}

func (w WindowV1) GetTimeRange(scheduleTime time.Time) (time.Time, time.Time, error) {
	err := w.Validate()
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	truncatedTime := w.truncateTime(scheduleTime)
	endTime := w.adjustOffset(truncatedTime)
	return w.getStartTime(endTime), endTime, nil
}

func (w WindowV1) truncateTime(scheduleTime time.Time) time.Time {
	if w.TruncateTo == "h" {
		// remove time upto hours
		return scheduleTime.Truncate(time.Hour)
	}
	if w.TruncateTo == "d" || w.TruncateTo == "" {
		// remove time upto day
		return scheduleTime.Truncate(24 * time.Hour)
	}
	if w.TruncateTo == "w" {
		// should truncate to the end of current week
		// weekday with start of the week as Sunday
		daysToAdd := time.Saturday + 1 - scheduleTime.Weekday()

		durationToAdd := time.Duration(daysToAdd) * 24 * time.Hour
		truncatedToDay := scheduleTime.Truncate(24 * time.Hour)
		return truncatedToDay.Add(durationToAdd)
	}
	if w.TruncateTo == "m" || w.TruncateTo == "M" {
		// should truncate to the end of the month.
		startOfCurrentMonth := time.Date(scheduleTime.Year(), scheduleTime.Month(), 1, 0, 0, 0, 0, time.UTC)
		return startOfCurrentMonth.AddDate(0, 1, 0)
	}
	return scheduleTime
}

func (w WindowV1) adjustOffset(truncatedTime time.Time) time.Time {
	if w.Offset == "" {
		return truncatedTime
	}
	duration, err := time.ParseDuration(w.Offset)
	if err != nil {
		return truncatedTime
	}
	return truncatedTime.Add(duration)
}

func (w WindowV1) getStartTime(endTime time.Time) time.Time {
	size := w.Size
	// default truncate to day
	if w.Size == "" {
		size = "24h"
	}

	// if monthly truncate measure the number of months based on hours(30 hours is one month)
	if w.TruncateTo == "m" || w.TruncateTo == "M" {
		hours, _ := strconv.Atoi(strings.Split(size, "h")[0])
		months := hours / 30
		return endTime.AddDate(0, -months, 0)
	}

	duration, err := time.ParseDuration(size)
	if err != nil {
		return endTime
	}
	return endTime.Add(-duration)
}

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
	//duration contains only month
	if len(splits) == 1 || splits[1] == "" {
		return months, "0", nil
	}
	//if duration is negative then use the negative duration for both the splits.
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
	if err != nil { //not expecting this, if this happens due to bad code just return inputTime
		return time.Time{}, err
	}
	return endTime.Add(-nonMonthDuration).AddDate(0, -months, 0), nil
}
