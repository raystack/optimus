package models

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	HoursInDay   = time.Hour * 24
	HoursInMonth = HoursInDay * 30
)

var monthExp = regexp.MustCompile("(\\+|-)?([0-9]+)(M)") //nolint:gosimple

type WindowV1 struct {
	TruncateTo string
	Offset     string
	Size       string
}

func (w WindowV1) Validate() error {
	_, _, _, err := w.getFieldValues()
	return err
}

func (w WindowV1) GetEndTime(scheduleTime time.Time) (endTime time.Time, err error) {
	truncateTo, offset, size, getErr := w.getFieldValues()
	if getErr != nil {
		err = getErr
		return
	}
	_, endTime = w.getTimeRange(scheduleTime, truncateTo, offset, size)
	return
}

func (w WindowV1) GetStartTime(scheduleTime time.Time) (startTime time.Time, err error) {
	truncateTo, offset, size, getErr := w.getFieldValues()
	if getErr != nil {
		err = getErr
		return
	}
	startTime, _ = w.getTimeRange(scheduleTime, truncateTo, offset, size)
	return
}

func (w WindowV1) GetTruncateTo() string {
	truncateTo, _, _, _ := w.getFieldValues()
	return truncateTo
}

func (w WindowV1) GetOffsetAsDuration() time.Duration {
	_, offset, _, _ := w.getFieldValues()
	return offset
}

func (w WindowV1) GetOffset() string {
	if w.Offset != "" {
		return w.Offset
	}
	return w.inHrs(int(w.GetOffsetAsDuration().Hours()))
}

func (w WindowV1) GetSizeAsDuration() time.Duration {
	_, _, size, _ := w.getFieldValues()
	return size
}

func (w WindowV1) GetSize() string {
	if w.Size != "" {
		return w.Size
	}
	return w.inHrs(int(w.GetSizeAsDuration().Hours()))
}

func (w WindowV1) inHrs(hrs int) string {
	if hrs == 0 {
		return "0"
	}
	return fmt.Sprintf("%dh", hrs)
}

func (w WindowV1) getTimeRange(scheduleTime time.Time, truncateTo string, offset, size time.Duration) (time.Time, time.Time) {
	floatingEnd := scheduleTime

	// apply truncation to end
	if truncateTo == "h" {
		// remove time upto hours
		floatingEnd = floatingEnd.Truncate(time.Hour)
	} else if truncateTo == "d" {
		// remove time upto day
		floatingEnd = floatingEnd.Truncate(HoursInDay)
	} else if truncateTo == "w" {
		// shift current window to nearest Sunday
		nearestSunday := time.Duration(time.Saturday-floatingEnd.Weekday()+1) * HoursInDay
		floatingEnd = floatingEnd.Add(nearestSunday)
		floatingEnd = floatingEnd.Truncate(HoursInDay)
	}

	windowEnd := floatingEnd.Add(offset)
	windowStart := windowEnd.Add(-size)

	// handle monthly windows separately as every month is not of same size
	if truncateTo == "M" {
		floatingEnd = scheduleTime
		// shift current window to nearest month start and end

		// truncate the date
		floatingEnd = time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)

		// then add the month offset
		// for handling offset, treat 30 days as 1 month
		offsetMonths := offset / HoursInMonth
		floatingEnd = floatingEnd.AddDate(0, int(offsetMonths), 0)

		// then find the last day of this month
		floatingEnd = floatingEnd.AddDate(0, 1, -1)

		// final end is computed
		windowEnd = floatingEnd.Truncate(HoursInDay)

		// truncate days/hours from window start as well
		floatingStart := time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)
		// for handling size, treat 30 days as 1 month, and as we have already truncated current month
		// subtract 1 from this
		sizeMonths := (size / HoursInMonth) - 1
		if sizeMonths > 0 {
			floatingStart = floatingStart.AddDate(0, int(-sizeMonths), 0)
		}

		// final start is computed
		windowStart = floatingStart
	}

	return windowStart, windowEnd
}

func (w WindowV1) getFieldValues() (truncateTo string, offsetAsDuration, sizeAsDuration time.Duration, err error) {
	truncateTo = "d"
	if w.TruncateTo != "" {
		truncateTo = w.TruncateTo
	}

	sizeAsDuration = HoursInDay
	if w.Size != "" {
		tempSize, sizeErr := w.tryParsing(w.Size)
		if sizeErr != nil {
			err = sizeErr
			return
		}
		sizeAsDuration = tempSize
	}

	if w.Offset != "" {
		tempOffset, offsetErr := w.tryParsing(w.Offset)
		if offsetErr != nil {
			err = offsetErr
			return
		}
		offsetAsDuration = tempOffset
	}
	return
}

func (w WindowV1) tryParsing(str string) (time.Duration, error) {
	var output time.Duration
	rst, err := w.tryParsingInMonths(str)
	if err != nil {
		return time.ParseDuration(str)
	} else {
		output = rst
	}
	return output, nil
}

func (w WindowV1) tryParsingInMonths(str string) (time.Duration, error) {
	sz := time.Duration(0)
	monthMatches := monthExp.FindAllStringSubmatch(str, -1)
	if len(monthMatches) > 0 && len(monthMatches[0]) == 4 {
		// replace month notation with days first, treating 1M as 30 days
		monthsCount, err := strconv.Atoi(monthMatches[0][2])
		if err != nil {
			return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
		}
		sz = HoursInMonth * time.Duration(monthsCount)
		if monthMatches[0][1] == "-" {
			sz *= -1
		}

		str = strings.TrimSpace(monthExp.ReplaceAllString(str, ""))
		if len(str) > 0 {
			// check if there is remaining time that we can still parse
			remainingTime, err := time.ParseDuration(str)
			if err != nil {
				return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
			}
			sz += remainingTime
		}
		return sz, nil
	}
	return sz, errors.New("invalid month string")
}
