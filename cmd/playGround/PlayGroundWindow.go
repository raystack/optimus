package playGround

import (
	"fmt"
	"time"

	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
	"github.com/spf13/cobra"
)

type Window struct {
	clientConfig         *config.ClientConfig
	survey               survey.WindowSurvey
	size                 int
	offset               int
	truncated            string
	sehduledDate         time.Time
	currentFinishingDate time.Time
}

// NewPlayGroundWindowCommand initializes command for window
func NewPlayGroundWindowCommand(clientConfig *config.ClientConfig) *cobra.Command {
	Window := Window{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:      "window",
		Short:    "get dStart,dEnd by giving the window params",
		PostRunE: Window.PostRunE,
		RunE:     Window.RunE,
	}
	return cmd
}
func (j *Window) PostRunE(_ *cobra.Command, _ []string) error {
	j.survey = *survey.NewwindowSurvey(j.survey.Logger)
	return nil
}
func (j *Window) RunE(_ *cobra.Command, _ []string) error {
	var state string = "Y"
	for state == "Y" {
		j.size = j.survey.GetWindowSize()
		j.offset = j.survey.GetOffsetSize()
		j.truncated = j.survey.GetTrucatedTo()
		j.sehduledDate = j.survey.GetSechduleDate()
		j.currentFinishingDate = j.sehduledDate
		j.trucate()
		j.applyoffset()
		j.printWindow()
		state = j.survey.GetStateInput()
	}
	return nil
}

// logic:- we trucate hour its its hour but we trucate both hour,day for day and we then trucate based on week and month
//depending on the input
func (j *Window) trucate() {
	if j.currentFinishingDate.Minute() != 0 {
		k := (60 - j.currentFinishingDate.Minute())
		remainingMinituesToTruncate := time.Duration(k) * (time.Minute)
		j.currentFinishingDate = j.currentFinishingDate.Add(remainingMinituesToTruncate)
	}
	//fmt.Println(j.currentFinishingDate)
	if j.truncated == "hour" {
		return
	}
	if j.currentFinishingDate.Hour() != 0 {
		k := 24 - j.currentFinishingDate.Hour()
		remainingHoursToTruncate := time.Duration(k) * (time.Hour)
		j.currentFinishingDate = j.currentFinishingDate.Add(remainingHoursToTruncate)
	}
	//fmt.Println(j.currentFinishingDate)
	if j.truncated == "day" {
		return
	}
	if j.truncated == "week" {
		if int(j.currentFinishingDate.Weekday()) != 1 {
			k := 8 - int(j.currentFinishingDate.Weekday())
			if k == 8 {
				k = 1
			}
			remainingHoursToTruncate := time.Duration(k*24) * (time.Hour)
			j.currentFinishingDate = j.currentFinishingDate.Add(remainingHoursToTruncate)
		}
		//fmt.Println(j.currentFinishingDate)
		return
	}
	if j.currentFinishingDate.Day() != 1 {
		j.currentFinishingDate = j.currentFinishingDate.AddDate(0, 1, 0)
	}
	j.currentFinishingDate = j.currentFinishingDate.AddDate(0, 0, -j.currentFinishingDate.Day()+1)
	//fmt.Println(j.currentFinishingDate)
}

// a mock function with same logic as truncate but for tests
func Mock_trucate(sechduledDate time.Time, truncated string) time.Time {
	if sechduledDate.Minute() != 0 {
		k := (60 - sechduledDate.Minute())
		remainingMinituesToTruncate := time.Duration(k) * (time.Minute)
		sechduledDate = sechduledDate.Add(remainingMinituesToTruncate)
	}
	if truncated == "hour" {
		return sechduledDate
	}
	if sechduledDate.Hour() != 0 {
		k := 24 - sechduledDate.Hour()
		remainingHoursToTruncate := time.Duration(k) * (time.Hour)
		sechduledDate = sechduledDate.Add(remainingHoursToTruncate)
	}
	if truncated == "day" {
		return sechduledDate
	}
	if truncated == "week" {
		if int(sechduledDate.Weekday()) != 1 {
			k := 8 - int(sechduledDate.Weekday())
			if k == 8 {
				k = 1
			}
			remainingHoursToTruncate := time.Duration(k*24) * (time.Hour)
			sechduledDate = sechduledDate.Add(remainingHoursToTruncate)
		}
		//fmt.Println(j.currentFinishingDate)
		return sechduledDate
	}
	if sechduledDate.Day() != 1 {
		sechduledDate = sechduledDate.AddDate(0, 1, 0)
	}
	sechduledDate = sechduledDate.AddDate(0, 0, -sechduledDate.Day()+1)
	return sechduledDate
}

// using add date for offset and calculating the original window
func (j *Window) applyoffset() {
	j.currentFinishingDate = j.currentFinishingDate.Add(time.Duration(-1*j.offset) * time.Hour)
}
func (j *Window) printWindow() {
	dStart := j.currentFinishingDate.Add(time.Duration(-1*j.size) * time.Hour)
	fmt.Println("strart time :", dStart, "    ", j.currentFinishingDate)
}
