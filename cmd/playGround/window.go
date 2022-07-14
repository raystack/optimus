package playground

// contins the main window logic
import (
	"fmt"
	"time"

	"github.com/odpf/optimus/cmd/survey"
)

type Window struct {
	survey survey.WindowSurvey
}

// logic:- we trucate hour its its hour
//we trucate both hour,day for day
//we trucate hour,day,week for week
//we trucate both hour,day,month for month
func (j *Window) truncate(currentFinishingDate time.Time, truncated string) time.Time {
	//add the remaining minitues to the nearest hour to round it up
	if currentFinishingDate.Minute() != 0 {
		k := currentFinishingDate.Minute()
		remainingMinituesToTruncate := time.Duration(k) * (time.Minute)
		currentFinishingDate = currentFinishingDate.Add(-1 * remainingMinituesToTruncate)
	}
	if truncated == "hour" {
		return currentFinishingDate
	}
	//add the remaining hours to the nearest day to round it up
	if currentFinishingDate.Hour() != 0 {
		k := currentFinishingDate.Hour()
		remainingHoursToTruncate := time.Duration(k) * (time.Hour)
		currentFinishingDate = currentFinishingDate.Add(-1 * remainingHoursToTruncate)
	}
	if truncated == "day" {
		return currentFinishingDate
	}
	// since we round for both day and hour we are sure that the time is 00:00 so we only have to grt to the nearest monday
	if truncated == "week" {
		if int(currentFinishingDate.Weekday()) != 1 {
			k := int(currentFinishingDate.Weekday())
			if k == 0 {
				k = 7
			}
			k--
			remainingHoursToTruncate := time.Duration(k*24) * (time.Hour)
			currentFinishingDate = currentFinishingDate.Add(-1 * remainingHoursToTruncate)
		}
		return currentFinishingDate
	}
	return time.Date(currentFinishingDate.Year(), currentFinishingDate.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// using add date for offset and calculating the original window
func (j *Window) applyoffset(currentFinishingDate time.Time, offset MonthHour) time.Time {
	currentFinishingDate = currentFinishingDate.AddDate(0, offset.month, 0).Add(time.Duration(offset.hour) * time.Hour)
	return currentFinishingDate
}

// printing the results
func (j *Window) printWindow(currentFinishingDate time.Time, size int) {
	dStart := currentFinishingDate.Add(time.Duration(-1*size) * time.Hour)
	fmt.Println("strart time :", dStart, "    ", currentFinishingDate)
}
