package playGround

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

type window struct {
	clientConfig         *config.ClientConfig
	survey               WindowSurvey
	size                 int
	offset               int
	truncated            string
	sehduledDate         time.Time
	currentFinishingDate time.Time
}

// NewPlayGroundWindowCommand initializes command for window
func NewPlayGroundWindowCommand(clientConfig *config.ClientConfig) *cobra.Command {
	window := window{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:      "window",
		Short:    "get dStart,dEnd by giving the window params",
		PostRunE: window.PostRunE,
		RunE:     window.RunE,
	}
	return cmd
}
func (j *window) PostRunE(_ *cobra.Command, _ []string) error {
	j.survey = *NewwindowSurvey(j.survey.logger)
	return nil
}
func (j *window) RunE(_ *cobra.Command, _ []string) error {
	j.size = j.survey.getWindowSize()
	j.offset = j.survey.getOffsetSize()
	j.truncated = j.survey.getTrucatedTo()
	j.sehduledDate = j.survey.getSechduleDate()
	j.currentFinishingDate = j.sehduledDate
	j.trucate()
	j.applyoffset()
	j.printWindow()
	return nil
}

// logic:- we trucate hour its its hour but we trucate both hour,day for day and we then trucate based on week and month
//depending on the input
func (j *window) trucate() {
	if j.currentFinishingDate.Minute() != 0 {
		k := (60 - j.currentFinishingDate.Minute())
		remainingMinituesToTruncate := time.Duration(k) * (time.Minute)
		j.currentFinishingDate = j.currentFinishingDate.Add(remainingMinituesToTruncate)
	}
	fmt.Println(j.currentFinishingDate)
	if j.truncated == "hour" {
		return
	}
	if j.currentFinishingDate.Hour() != 0 {
		k := 24 - j.currentFinishingDate.Hour()
		remainingHoursToTruncate := time.Duration(k) * (time.Hour)
		j.currentFinishingDate = j.currentFinishingDate.Add(remainingHoursToTruncate)
	}
	fmt.Println(j.currentFinishingDate)
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
		fmt.Println(j.currentFinishingDate)
		return
	}
	if j.currentFinishingDate.Day() != 1 {
		j.currentFinishingDate = j.currentFinishingDate.AddDate(0, 1, 0)
	}
	j.currentFinishingDate = j.currentFinishingDate.AddDate(0, 0, -j.currentFinishingDate.Day()+1)
	fmt.Println(j.currentFinishingDate)
}

// using add date for offset and calculating the original window
func (j *window) applyoffset() {
	j.currentFinishingDate = j.currentFinishingDate.Add(time.Duration(-1*j.offset) * time.Hour)
}
func (j *window) printWindow() {
	dStart := j.currentFinishingDate.Add(time.Duration(-1*j.size) * time.Hour)
	fmt.Println("strart time :", dStart, "    ", j.currentFinishingDate)
}
