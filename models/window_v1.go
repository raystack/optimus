package models

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/odpf/optimus/utils"
)

const (
	HoursInDay   = time.Hour * 24
	HoursInMonth = 30 * 24 * time.Hour
)

var monthExp = regexp.MustCompile("(\\+|-)?([0-9]+)(M)") //nolint:gosimple

type WindowV1 struct {
	TruncateTo string

	Offset           string
	OffsetAsDuration time.Duration

	Size           string
	SizeAsDuration time.Duration
}

func (w WindowV1) Validate() error {
	validTruncateOptions := []string{"h", "d", "w", "m", "M"}
	if !utils.ContainsString(validTruncateOptions, w.TruncateTo) {
		return fmt.Errorf("invalid option provided, provide one of : %v", validTruncateOptions)
	}
	return w.prepareWindow()
}

func (w WindowV1) GetStartTime(scheduleTime time.Time) (time.Time, error) {
	if err := w.Validate(); err != nil {
		return time.Time{}, err
	}
	start, _ := w.getWindowDate(scheduleTime)
	return start, nil
}

func (w WindowV1) GetEndTime(scheduleTime time.Time) (time.Time, error) {
	if err := w.Validate(); err != nil {
		return time.Time{}, err
	}
	_, end := w.getWindowDate(scheduleTime)
	return end, nil
}

func (w WindowV1) GetTruncateTo() string {
	return w.TruncateTo
}

func (w WindowV1) GetOffsetAsDuration() time.Duration {
	return w.OffsetAsDuration
}

func (w WindowV1) GetOffset() string {
	return w.Offset
}

func (w WindowV1) GetSizeAsDuration() time.Duration {
	return w.SizeAsDuration
}

func (w WindowV1) GetSize() string {
	return w.Size
}

func (w WindowV1) getWindowDate(scheduleTime time.Time) (time.Time, time.Time) {
	floatingEnd := scheduleTime
	// apply truncation to end
	if w.TruncateTo == "h" {
		// remove time upto hours
		floatingEnd = scheduleTime.Truncate(time.Hour)
	} else if w.TruncateTo == "d" {
		// remove time upto day
		floatingEnd = floatingEnd.Truncate(HoursInDay)
	} else if w.TruncateTo == "w" {
		// shift current window to nearest Sunday
		nearestSunday := time.Duration(time.Saturday-floatingEnd.Weekday()+1) * HoursInDay
		floatingEnd = floatingEnd.Add(nearestSunday)
		floatingEnd = floatingEnd.Truncate(HoursInDay)
	}

	windowEnd := floatingEnd.Add(w.OffsetAsDuration)
	windowStart := windowEnd.Add(-w.SizeAsDuration)

	// handle monthly windows separately as every month is not of same size
	if w.TruncateTo == "M" || w.TruncateTo == "m" {
		floatingEnd = scheduleTime
		// shift current window to nearest month start and end

		// truncate the date
		floatingEnd = time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)

		// then add the month offset
		// for handling offset, treat 30 days as 1 month
		offsetMonths := w.OffsetAsDuration / HoursInMonth
		floatingEnd = floatingEnd.AddDate(0, int(offsetMonths), 0)

		// then find the last day of this month
		floatingEnd = floatingEnd.AddDate(0, 1, -1)

		// final end is computed
		windowEnd = floatingEnd.Truncate(HoursInDay)

		// truncate days/hours from window start as well
		floatingStart := time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)
		// for handling size, treat 30 days as 1 month, and as we have already truncated current month
		// subtract 1 from this
		sizeMonths := (w.SizeAsDuration / HoursInMonth) - 1
		if sizeMonths > 0 {
			floatingStart = floatingStart.AddDate(0, int(-sizeMonths), 0)
		}

		// final start is computed
		windowStart = floatingStart
	}

	return windowStart, windowEnd
}

func (w WindowV1) prepareWindow() error {
	var truncateTo string = "d"
	if w.TruncateTo != "" {
		truncateTo = w.TruncateTo
	}

	var size time.Duration = HoursInDay
	if w.SizeAsDuration == 0 && w.Size != "" {
		tempSize, err := w.tryParsingInMonths(w.Size)
		if err != nil {
			tempSize, err := time.ParseDuration(w.Size)
			if err != nil {
				return fmt.Errorf("failed to parse task window with size %v: %w", w.SizeAsDuration, err)
			}
			size = tempSize
		}
		size = tempSize
	}

	var offset time.Duration = 0
	if w.OffsetAsDuration == 0 && w.Offset != "" {
		tempOffset, err := w.tryParsingInMonths(w.Offset)
		if err != nil {
			tempOffset, err := time.ParseDuration(w.Offset)
			if err != nil {
				return fmt.Errorf("failed to parse task window with offset %v: %w", w.OffsetAsDuration, err)
			}
			offset = tempOffset
		}
		offset = tempOffset
	}

	w.TruncateTo = truncateTo
	w.SizeAsDuration = size
	w.OffsetAsDuration = offset
	return nil
}

// check if string contains monthly notation
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
