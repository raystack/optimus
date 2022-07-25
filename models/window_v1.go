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

type windowV1 struct {
	size       string
	offset     string
	truncateTo string
}

func (w windowV1) Validate() error {
	_, err := w.prepareWindow()
	if err != nil {
		return err
	}
	if w.size != "" {
		_, err := time.ParseDuration(w.size)
		if err != nil {
			return fmt.Errorf("failed to parse task window with size %v: %w", w.size, err)
		}
		if strings.HasPrefix(w.size, "-") {
			return fmt.Errorf("size cannot be negative, %s", w.size)
		}
	}
	if w.offset != "" {
		_, err := time.ParseDuration(w.offset)
		if err != nil {
			return fmt.Errorf("failed to parse task window with size %v: %w", w.offset, err)
		}
	}
	if w.truncateTo != "" {
		validTruncateOptions := []string{"h", "d", "w", "m", "M"}
		if utils.ContainsString(validTruncateOptions, w.truncateTo) == false {
			return fmt.Errorf("invalid option provided, provide one of : %v", validTruncateOptions)
		}
	}
	return nil
}

func (w windowV1) GetStartTime(scheduleTime time.Time) (time.Time, error) {
	if err := w.Validate(); err != nil {
		return time.Time{}, err
	}
	jobWindow, err := w.prepareWindow()
	if err != nil {
		return time.Time{}, err
	}
	return jobWindow.GetStart(scheduleTime), nil
}

func (w windowV1) GetEndTime(scheduleTime time.Time) (time.Time, error) {
	if err := w.Validate(); err != nil {
		return time.Time{}, err
	}
	jobWindow, err := w.prepareWindow()
	if err != nil {
		return time.Time{}, err
	}
	return jobWindow.GetEnd(scheduleTime), nil
}

func (w windowV1) GetTruncateTo() string {
	return w.truncateTo
}

func (w windowV1) GetOffset() string {
	return w.offset
}

func (w windowV1) GetSize() string {
	return w.size
}

func (w windowV1) prepareWindow() (JobSpecTaskWindow, error) {
	var err error
	window := JobSpecTaskWindow{}
	window.Size = HoursInDay
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

// check if string contains monthly notation
func (w windowV1) tryParsingInMonths(str string) (time.Duration, error) {
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
