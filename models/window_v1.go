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
	HoursInDay   = 24
	HoursInMonth = HoursInDay * 30
)

var monthExp = regexp.MustCompile("(\\+|-)?([0-9]+)(M)") //nolint:gosimple

type windowV1 struct {
	truncateTo string
	offset     string
	size       string
}

func (w windowV1) Validate() error {
	_, err := w.prepareWindow() // nolint:dogsled
	return err
}

func (w windowV1) GetTruncateTo() string {
	return w.truncateTo
}

func (w windowV1) GetOffset() string {
	if w.offset != "" {
		return w.offset
	}
	return w.inHrs(0)
}

func (w windowV1) GetSize() string {
	if w.size != "" {
		return w.size
	}
	return w.inHrs(HoursInDay)
}

func (w windowV1) GetStartTime(scheduledAt time.Time) (startTime time.Time, err error) {
	jobSpecTaskWindow, err := w.prepareWindow()
	if err != nil {
		return
	}
	startTime, _ = jobSpecTaskWindow.getWindowDate(scheduledAt, jobSpecTaskWindow.Size, jobSpecTaskWindow.Offset, jobSpecTaskWindow.TruncateTo)
	return
}

func (w windowV1) GetEndTime(scheduledAt time.Time) (endTime time.Time, err error) {
	jobSpecTaskWindow, err := w.prepareWindow()
	if err != nil {
		return
	}
	_, endTime = jobSpecTaskWindow.getWindowDate(scheduledAt, jobSpecTaskWindow.Size, jobSpecTaskWindow.Offset, jobSpecTaskWindow.TruncateTo)
	return
}

type JobSpecTaskWindow struct {
	Size       time.Duration
	Offset     time.Duration
	TruncateTo string
}

func (w *windowV1) prepareWindow() (JobSpecTaskWindow, error) {
	var err error
	window := JobSpecTaskWindow{}
	window.Size = time.Hour * HoursInDay
	window.Offset = 0
	window.TruncateTo = "d"

	if w.truncateTo != "" {
		window.TruncateTo = w.truncateTo
	}

	// check if string contains monthly notation
	if w.size != "" {
		window.Size, err = w.tryParsingInMonths(w.size)
		if err != nil {
			// treat as normal duration
			window.Size, err = time.ParseDuration(w.size)
			if err != nil {
				return window, fmt.Errorf("failed to parse task window with size %v: %w", w.size, err)
			}
		}
	}

	// check if string contains monthly notation
	if w.offset != "" {
		window.Offset, err = w.tryParsingInMonths(w.offset)
		if err != nil {
			// treat as normal duration
			window.Offset, err = time.ParseDuration(w.offset)
			if err != nil {
				return window, fmt.Errorf("failed to parse task window with offset %v: %w", w.offset, err)
			}
		}
	}

	return window, nil
}

func (*JobSpecTaskWindow) getWindowDate(today time.Time, windowSize, windowOffset time.Duration, windowTruncateTo string) (time.Time, time.Time) {
	floatingEnd := today

	// apply truncation to end
	if windowTruncateTo == "h" {
		// remove time upto hours
		floatingEnd = floatingEnd.Truncate(time.Hour)
	} else if windowTruncateTo == "d" {
		// remove time upto day
		floatingEnd = floatingEnd.Truncate(time.Hour * HoursInDay)
	} else if windowTruncateTo == "w" {
		// shift current window to nearest Sunday
		nearestSunday := time.Duration(time.Saturday-floatingEnd.Weekday()+1) * time.Hour * HoursInDay
		floatingEnd = floatingEnd.Add(nearestSunday)
		floatingEnd = floatingEnd.Truncate(time.Hour * HoursInDay)
	}

	windowEnd := floatingEnd.Add(windowOffset)
	windowStart := windowEnd.Add(-windowSize)

	// handle monthly windows separately as every month is not of same size
	if windowTruncateTo == "M" || windowTruncateTo == "m" {
		floatingEnd = today
		// shift current window to nearest month start and end

		// truncate the date
		floatingEnd = time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)

		// then add the month offset
		// for handling offset, treat 30 days as 1 month
		offsetMonths := windowOffset / (time.Hour * HoursInMonth)
		floatingEnd = floatingEnd.AddDate(0, int(offsetMonths), 0)

		// then find the last day of this month
		floatingEnd = floatingEnd.AddDate(0, 1, -1)

		// final end is computed
		windowEnd = floatingEnd.Truncate(time.Hour * HoursInDay)

		// truncate days/hours from window start as well
		floatingStart := time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)
		// for handling size, treat 30 days as 1 month, and as we have already truncated current month
		// subtract 1 from this
		sizeMonths := (windowSize / (time.Hour * HoursInMonth)) - 1
		if sizeMonths > 0 {
			floatingStart = floatingStart.AddDate(0, int(-sizeMonths), 0)
		}

		// final start is computed
		windowStart = floatingStart
	}

	return windowStart, windowEnd
}

func (windowV1) inHrs(hrs int) string {
	if hrs == 0 {
		return "0"
	}
	return fmt.Sprintf("%dh", hrs)
}

// check if string contains monthly notation
func (windowV1) tryParsingInMonths(str string) (time.Duration, error) {
	sz := time.Duration(0)
	monthMatches := monthExp.FindAllStringSubmatch(str, -1)
	if len(monthMatches) > 0 && len(monthMatches[0]) == 4 {
		// replace month notation with days first, treating 1M as 30 days
		monthsCount, err := strconv.Atoi(monthMatches[0][2])
		if err != nil {
			return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
		}
		sz = time.Hour * HoursInMonth * time.Duration(monthsCount)
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
