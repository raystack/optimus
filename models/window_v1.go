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

type WindowV1Scope string

const (
	ScopeClient WindowV1Scope = "client"
	ScopeServer WindowV1Scope = "server"
)

type WindowV1 struct {
	Scope WindowV1Scope

	TruncateTo string

	Offset           string
	OffsetAsDuration time.Duration

	Size           string
	SizeAsDuration time.Duration
}

func (w *WindowV1) Enrich() error {
	if err := w.Validate(); err != nil {
		return err
	}
	truncateTo := w.GetTruncateTo()
	offset := w.GetOffset()
	offsetAsDuration := w.GetOffsetAsDuration()
	size := w.GetSize()
	sizeAsDuration := w.GetSizeAsDuration()

	w.TruncateTo = truncateTo
	w.Offset = offset
	w.OffsetAsDuration = offsetAsDuration
	w.Size = size
	w.SizeAsDuration = sizeAsDuration
	return nil
}

func (w *WindowV1) Validate() error {
	_, _, _, err := w.getFieldValues()
	return err
}

func (w *WindowV1) GetEndTime(scheduleTime time.Time) (endTime time.Time, err error) {
	truncateTo, offset, size, getErr := w.getFieldValues()
	if getErr != nil {
		err = getErr
		return
	}
	_, endTime = w.getTimeRange(scheduleTime, truncateTo, offset, size)
	return
}

func (w *WindowV1) GetStartTime(scheduleTime time.Time) (startTime time.Time, err error) {
	truncateTo, offset, size, getErr := w.getFieldValues()
	if getErr != nil {
		err = getErr
		return
	}
	startTime, _ = w.getTimeRange(scheduleTime, truncateTo, offset, size)
	return
}

func (w *WindowV1) GetTruncateTo() string {
	truncateTo, _, _, _ := w.getFieldValues()
	return truncateTo
}

func (w *WindowV1) GetOffsetAsDuration() time.Duration {
	_, offset, _, _ := w.getFieldValues()
	return offset
}

func (w *WindowV1) GetOffset() string {
	return w.inHrs(int(w.GetOffsetAsDuration().Hours()))
}

func (w *WindowV1) GetSizeAsDuration() time.Duration {
	_, _, size, _ := w.getFieldValues()
	return size
}

func (w *WindowV1) GetSize() string {
	return w.inHrs(int(w.GetSizeAsDuration().Hours()))
}

func (w *WindowV1) inHrs(hrs int) string {
	if hrs == 0 {
		return "0"
	}
	return fmt.Sprintf("%dh", hrs)
}

func (w *WindowV1) getTimeRange(scheduleTime time.Time, truncateTo string, offset, size time.Duration) (time.Time, time.Time) {
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

func (w *WindowV1) getFieldValues() (truncateTo string, offsetAsDuration time.Duration, sizeAsDuration time.Duration, err error) {
	switch w.Scope {
	case ScopeClient:
		truncateTo, offsetAsDuration, sizeAsDuration, err = w.getFieldValuesForClient()
		if err != nil {
			return
		}
	case ScopeServer:
		truncateTo, offsetAsDuration, sizeAsDuration, err = w.getFieldValuesForServer()
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("scope [%s] is not recognized", w.Scope)
	}
	return
}

func (w *WindowV1) getFieldValuesForClient() (truncateTo string, offsetAsDuration, sizeAsDuration time.Duration, err error) {
	truncateTo = "d"
	if w.TruncateTo != "" {
		truncateTo = w.TruncateTo
	}

	sizeAsDuration = HoursInDay
	if w.Size != "" {
		tempSize, sizeErr := w.tryParsingInMonths(w.Size)
		if sizeErr != nil {
			tempSize, sizeErr := time.ParseDuration(w.Size)
			if sizeErr != nil {
				err = sizeErr
				return
			}
			sizeAsDuration = tempSize
		} else {
			sizeAsDuration = tempSize
		}
	}

	offsetAsDuration = HoursInDay
	if w.Offset != "" {
		tempOffset, offsetErr := w.tryParsingInMonths(w.Offset)
		if offsetErr != nil {
			tempOffset, sizeErr := time.ParseDuration(w.Offset)
			if sizeErr != nil {
				err = sizeErr
				return
			}
			offsetAsDuration = tempOffset
		} else {
			offsetAsDuration = tempOffset
		}
	}
	return
}

func (w *WindowV1) tryParsingInMonths(str string) (time.Duration, error) {
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

func (w *WindowV1) getFieldValuesForServer() (truncateTo string, offsetAsDuration, sizeAsDuration time.Duration, err error) {
	truncateTo = "d"
	if w.TruncateTo != "" {
		truncateTo = w.TruncateTo
	}

	if w.SizeAsDuration != 0 {
		sizeAsDuration = w.SizeAsDuration
	} else {
		sizeAsDuration = HoursInDay
		if w.Size != "" {
			tempSize, sizeErr := time.ParseDuration(w.Size)
			if sizeErr != nil {
				err = sizeErr
				return
			}
			sizeAsDuration = tempSize
		}
	}

	if w.OffsetAsDuration != 0 {
		offsetAsDuration = w.OffsetAsDuration
	} else if w.Offset != "" {
		tempOffset, offsetErr := time.ParseDuration(w.Offset)
		if offsetErr != nil {
			err = offsetErr
			return
		}
		offsetAsDuration = tempOffset
	}
	return
}

// func (w *WindowV1) Validate() error {
// 	if w.GetSizeAsDuration() == 0 && w.GetSize() != "" {
// 		if _, err := w.getSizeAsDuration(); err != nil {
// 			return err
// 		}
// 	}
// 	if w.GetOffsetAsDuration() == 0 && w.GetOffset() != "" {
// 		if _, err := w.getOffsetAsDuration(); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (w *WindowV1) GetStartTime(scheduleTime time.Time) (time.Time, error) {
// 	if err := w.Validate(); err != nil {
// 		return time.Time{}, err
// 	}
// 	start, _ := w.getWindowDate(scheduleTime)
// 	return start, nil
// }

// func (w *WindowV1) GetEndTime(scheduleTime time.Time) (time.Time, error) {
// 	if err := w.Validate(); err != nil {
// 		return time.Time{}, err
// 	}
// 	_, end := w.getWindowDate(scheduleTime)
// 	return end, nil
// }

// func (w *WindowV1) GetTruncateTo() string {
// 	return w.TruncateTo
// }

// func (w *WindowV1) GetOffsetAsDuration() time.Duration {
// 	offset, _ := w.getOffsetAsDuration()
// 	return offset
// }

// func (w *WindowV1) GetOffset() string {
// 	return w.inHrs(int(w.GetOffsetAsDuration()))
// }

// func (w *WindowV1) GetSizeAsDuration() time.Duration {
// 	size, _ := w.getSizeAsDuration()
// 	return size
// }

// func (w *WindowV1) GetSize() string {
// 	return w.inHrs(int(w.GetSizeAsDuration()))
// }

// func (w *WindowV1) getOffsetAsDuration() (time.Duration, error) {
// 	return w.tryParsing(w.Offset)
// }

// func (w *WindowV1) getSizeAsDuration() (time.Duration, error) {
// 	return w.tryParsing(w.Size)
// }

// func (w *WindowV1) inHrs(hrs int) string {
// 	if hrs == 0 {
// 		return "0"
// 	}
// 	return fmt.Sprintf("%dh", hrs)
// }

// func (w *WindowV1) getWindowDate(scheduleTime time.Time) (time.Time, time.Time) {
// 	floatingEnd := scheduleTime
// 	// apply truncation to end
// 	if w.GetTruncateTo() == "h" {
// 		// remove time upto hours
// 		floatingEnd = scheduleTime.Truncate(time.Hour)
// 	} else if w.GetTruncateTo() == "d" {
// 		// remove time upto day
// 		floatingEnd = floatingEnd.Truncate(HoursInDay)
// 	} else if w.GetTruncateTo() == "w" {
// 		// shift current window to nearest Sunday
// 		nearestSunday := time.Duration(time.Saturday-floatingEnd.Weekday()+1) * HoursInDay
// 		floatingEnd = floatingEnd.Add(nearestSunday)
// 		floatingEnd = floatingEnd.Truncate(HoursInDay)
// 	}

// 	windowEnd := floatingEnd.Add(w.GetOffsetAsDuration())
// 	windowStart := windowEnd.Add(-w.GetSizeAsDuration())

// 	// handle monthly windows separately as every month is not of same size
// 	if w.GetTruncateTo() == "M" || w.GetTruncateTo() == "m" {
// 		floatingEnd = scheduleTime
// 		// shift current window to nearest month start and end

// 		// truncate the date
// 		floatingEnd = time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)

// 		// then add the month offset
// 		// for handling offset, treat 30 days as 1 month
// 		offsetMonths := w.GetOffsetAsDuration() / HoursInMonth
// 		floatingEnd = floatingEnd.AddDate(0, int(offsetMonths), 0)

// 		// then find the last day of this month
// 		floatingEnd = floatingEnd.AddDate(0, 1, -1)

// 		// final end is computed
// 		windowEnd = floatingEnd.Truncate(HoursInDay)

// 		// truncate days/hours from window start as well
// 		floatingStart := time.Date(floatingEnd.Year(), floatingEnd.Month(), 1, 0, 0, 0, 0, time.UTC)
// 		// for handling size, treat 30 days as 1 month, and as we have already truncated current month
// 		// subtract 1 from this
// 		sizeMonths := (w.GetSizeAsDuration() / HoursInMonth) - 1
// 		if sizeMonths > 0 {
// 			floatingStart = floatingStart.AddDate(0, int(-sizeMonths), 0)
// 		}

// 		// final start is computed
// 		windowStart = floatingStart
// 	}

// 	return windowStart, windowEnd
// }

// func (w *WindowV1) tryParsing(str string) (time.Duration, error) {
// 	duration, err := w.tryParsingInMonths(str)
// 	if err != nil {
// 		return time.ParseDuration(w.Size)
// 	}
// 	return duration, nil
// }

// // check if string contains monthly notation
// func (w *WindowV1) tryParsingInMonths(str string) (time.Duration, error) {
// 	sz := time.Duration(0)
// 	monthMatches := monthExp.FindAllStringSubmatch(str, -1)
// 	if len(monthMatches) > 0 && len(monthMatches[0]) == 4 {
// 		// replace month notation with days first, treating 1M as 30 days
// 		monthsCount, err := strconv.Atoi(monthMatches[0][2])
// 		if err != nil {
// 			return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
// 		}
// 		sz = HoursInMonth * time.Duration(monthsCount)
// 		if monthMatches[0][1] == "-" {
// 			sz *= -1
// 		}

// 		str = strings.TrimSpace(monthExp.ReplaceAllString(str, ""))
// 		if len(str) > 0 {
// 			// check if there is remaining time that we can still parse
// 			remainingTime, err := time.ParseDuration(str)
// 			if err != nil {
// 				return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
// 			}
// 			sz += remainingTime
// 		}
// 		return sz, nil
// 	}
// 	return sz, errors.New("invalid month string")
// }
