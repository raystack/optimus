package job_run

import (
	"strings"
	"time"

	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/utils"
)

type windowV2 struct {
	truncateTo string
	offset     string
	size       string
}

func (windowV2) GetVersion() int {
	return 2 //nolint:gomnd
}

func (w windowV2) Validate() error {
	if err := w.validateTruncateTo(); err != nil {
		return err
	}
	if err := w.validateOffset(); err != nil {
		return err
	}
	if err := w.validateSize(); err != nil {
		return err
	}
	return nil
}

func (w windowV2) GetStartTime(scheduleTime time.Time) (time.Time, error) {
	endTime, err := w.GetEndTime(scheduleTime)
	if err != nil {
		return time.Time{}, err
	}
	return w.getStartTime(endTime)
}

func (w windowV2) GetEndTime(scheduleTime time.Time) (time.Time, error) {
	if err := w.Validate(); err != nil {
		return time.Time{}, err
	}
	truncatedTime := w.truncateTime(scheduleTime)
	return w.adjustOffset(truncatedTime)
}

func (w windowV2) GetTruncateTo() string {
	return w.truncateTo
}

func (w windowV2) GetOffset() string {
	return w.offset
}

func (w windowV2) GetSize() string {
	return w.size
}

func (w windowV2) validateTruncateTo() error {
	if w.truncateTo == "" {
		return nil
	}

	validTruncateOptions := []string{"h", "d", "w", "M"}
	// TODO: perhaps we can avoid using util, in hope we can remove this package
	if !utils.ContainsString(validTruncateOptions, w.truncateTo) {
		return errors.InvalidArgument(EntityWindow, "invalid truncate_to provided, provide one of "+strings.Join(validTruncateOptions, ", "))
	}
	return nil
}

func (w windowV2) validateOffset() error {
	if w.offset == "" {
		return nil
	}

	_, nonMonthDuration, err := monthsAndNonMonthExpression(w.offset)
	if err != nil {
		return err
	}
	if nonMonthDuration != "" {
		if _, err = time.ParseDuration(nonMonthDuration); err != nil {
			return errors.InvalidArgument(EntityWindow, "failed to parse non month duration "+nonMonthDuration)
		}
	}
	return nil
}

func (w windowV2) validateSize() error {
	if w.size == "" {
		return nil
	}

	months, nonMonthDuration, err := monthsAndNonMonthExpression(w.size)
	if err != nil {
		return err
	}
	if months < 0 {
		return errors.InvalidArgument(EntityWindow, "size cannot be negative")
	}
	if nonMonthDuration != "" {
		if _, err := time.ParseDuration(nonMonthDuration); err != nil {
			return errors.InvalidArgument(EntityWindow, "failed to parse task window with size "+w.size)
		}
	}
	if strings.HasPrefix(w.size, "-") {
		return errors.InvalidArgument(EntityWindow, "size cannot be negative")
	}
	return nil
}

func (w windowV2) truncateTime(scheduleTime time.Time) time.Time {
	numberOfHoursInADay := 24
	if w.truncateTo == "" {
		return scheduleTime
	}
	if w.truncateTo == "h" {
		// remove time upto hours
		return scheduleTime.Truncate(time.Hour)
	}
	if w.truncateTo == "d" {
		// remove time upto day
		return scheduleTime.Truncate(time.Duration(numberOfHoursInADay) * time.Hour)
	}
	if w.truncateTo == "w" {
		truncatedToDay := scheduleTime.Truncate(time.Duration(numberOfHoursInADay) * time.Hour)
		// weekday with start of the week as Monday
		weekday := scheduleTime.Weekday()
		if weekday == 0 {
			weekday = 7
		}
		daysToSubtract := weekday - time.Monday

		durationToSubtract := time.Duration(daysToSubtract) * time.Duration(numberOfHoursInADay) * time.Hour
		return truncatedToDay.Add(-durationToSubtract)
	}
	if w.truncateTo == "M" {
		return time.Date(scheduleTime.Year(), scheduleTime.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return scheduleTime
}

func (w windowV2) adjustOffset(truncatedTime time.Time) (time.Time, error) {
	if w.offset == "" {
		return truncatedTime, nil
	}
	months, nonMonthDurationString, err := monthsAndNonMonthExpression(w.offset)
	if err != nil {
		return time.Time{}, err
	}

	nonMonthDuration, err := time.ParseDuration(nonMonthDurationString)
	if err != nil {
		return time.Time{}, errors.InvalidArgument(EntityWindow, "failed to parse non month duration "+nonMonthDurationString)
	}
	return truncatedTime.Add(nonMonthDuration).AddDate(0, months, 0), nil
}

func (w windowV2) getStartTime(endTime time.Time) (time.Time, error) {
	if w.size == "" {
		return endTime, nil
	}
	months, nonMonthDurationString, err := monthsAndNonMonthExpression(w.size)
	if err != nil {
		return time.Time{}, err
	}
	nonMonthDuration, err := time.ParseDuration(nonMonthDurationString)
	if err != nil { // not expecting this, if this happens due to bad code just return inputTime
		return time.Time{}, errors.InvalidArgument(EntityWindow, "failed to parse non month duration "+nonMonthDurationString)
	}
	return endTime.Add(-nonMonthDuration).AddDate(0, -months, 0), nil
}
