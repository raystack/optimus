package job

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

const (
	HoursInDay   = time.Hour * 24
	HoursInMonth = 30 * 24 * time.Hour
)

var monthExp = regexp.MustCompile("(\\+|-)?([0-9]+)(M)") //nolint:gosimple

type WindowV1 struct {
	Size       string
	Offset     string
	TruncateTo string
}

func (w WindowV1) Validate() error {
	_, err := w.prepareWindow()
	if err != nil {
		return err
	}
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
	jobWindow, err := w.prepareWindow()
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	startTime := jobWindow.GetStart(scheduleTime)
	endTime := jobWindow.GetEnd(scheduleTime)
	return startTime, endTime, nil
}

func (w WindowV1) prepareWindow() (models.JobSpecTaskWindow, error) {
	var err error
	window := models.JobSpecTaskWindow{}
	window.Size = HoursInDay
	window.Offset = 0
	window.TruncateTo = "d"

	if w.TruncateTo != "" {
		window.TruncateTo = w.TruncateTo
	}

	// check if string contains monthly notation
	if w.Size != "" {
		window.Size, err = w.tryParsingInMonths(w.Size)
		if err != nil {
			// treat as normal duration
			window.Size, err = time.ParseDuration(w.Size)
			if err != nil {
				return window, fmt.Errorf("failed to parse task window with size %v: %w", w.Size, err)
			}
		}
	}

	// check if string contains monthly notation
	if w.Offset != "" {
		window.Offset, err = w.tryParsingInMonths(w.Offset)
		if err != nil {
			// treat as normal duration
			window.Offset, err = time.ParseDuration(w.Offset)
			if err != nil {
				return window, fmt.Errorf("failed to parse task window with offset %v: %w", w.Offset, err)
			}
		}
	}

	return window, nil
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
