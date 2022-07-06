package playground

import (
	"fmt"
	"time"

	"github.com/odpf/optimus/cmd/survey"
)

type Window struct {
	survey survey.WindowSurvey
}

// logic:- we trucate hour its its hour but we trucate both hour,day for day and we then trucate based on week and month
//depending on the input
func (j *Window) truncate(currentFinishingDate time.Time, truncated string) time.Time {
	if currentFinishingDate.Minute() != 0 {
		k := (60 - currentFinishingDate.Minute())
		remainingMinituesToTruncate := time.Duration(k) * (time.Minute)
		currentFinishingDate = currentFinishingDate.Add(remainingMinituesToTruncate)
	}
	if truncated == "hour" {
		return currentFinishingDate
	}
	if currentFinishingDate.Hour() != 0 {
		k := 24 - currentFinishingDate.Hour()
		remainingHoursToTruncate := time.Duration(k) * (time.Hour)
		currentFinishingDate = currentFinishingDate.Add(remainingHoursToTruncate)
	}
	if truncated == "day" {
		return currentFinishingDate
	}
	if truncated == "week" {
		if int(currentFinishingDate.Weekday()) != 1 {
			k := 8 - int(currentFinishingDate.Weekday())
			if k == 8 {
				k = 1
			}
			remainingHoursToTruncate := time.Duration(k*24) * (time.Hour)
			currentFinishingDate = currentFinishingDate.Add(remainingHoursToTruncate)
		}
		return currentFinishingDate
	}
	if currentFinishingDate.Day() != 1 {
		currentFinishingDate = currentFinishingDate.AddDate(0, 1, 0)
	}
	currentFinishingDate = currentFinishingDate.AddDate(0, 0, -currentFinishingDate.Day()+1)
	return currentFinishingDate
}

// using add date for offset and calculating the original window
func (j *Window) applyoffset(currentFinishingDate time.Time, offset int) time.Time {
	currentFinishingDate = currentFinishingDate.Add(time.Duration(-1*offset) * time.Hour)
	return currentFinishingDate
}
func (j *Window) printWindow(currentFinishingDate time.Time, size int) {
	dStart := currentFinishingDate.Add(time.Duration(-1*size) * time.Hour)
	fmt.Println("strart time :", dStart, "    ", currentFinishingDate)
}
