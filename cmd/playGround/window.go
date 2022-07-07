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
		k := (60 - currentFinishingDate.Minute())
		remainingMinituesToTruncate := time.Duration(k) * (time.Minute)
		currentFinishingDate = currentFinishingDate.Add(remainingMinituesToTruncate)
	}
	if truncated == "hour" {
		return currentFinishingDate
	}
	//add the remaining hours to the nearest day to round it up
	if currentFinishingDate.Hour() != 0 {
		k := 24 - currentFinishingDate.Hour()
		remainingHoursToTruncate := time.Duration(k) * (time.Hour)
		currentFinishingDate = currentFinishingDate.Add(remainingHoursToTruncate)
	}
	if truncated == "day" {
		return currentFinishingDate
	}
	// since we round for both day and hour we are sure that the time is 00:00 so we only have to grt to the nearest monday
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
	// we use the same logic as the week here excep we round uo to 1 st of every month
	if currentFinishingDate.Day() != 1 {
		currentFinishingDate = currentFinishingDate.AddDate(0, 1, 0)
	}
	currentFinishingDate = currentFinishingDate.AddDate(0, 0, -currentFinishingDate.Day()+1)
	return currentFinishingDate
	// note that here although we write 1st 00:00 it can also be treated as end of last month as 31st 24:00
}

// using add date for offset and calculating the original window
func (j *Window) applyoffset(currentFinishingDate time.Time, offset int) time.Time {
	currentFinishingDate = currentFinishingDate.Add(time.Duration(-1*offset) * time.Hour)
	return currentFinishingDate
}
// printing the results
func (j *Window) printWindow(currentFinishingDate time.Time, size int) {
	dStart := currentFinishingDate.Add(time.Duration(-1*size) * time.Hour)
	fmt.Println("strart time :", dStart, "    ", currentFinishingDate)
}
