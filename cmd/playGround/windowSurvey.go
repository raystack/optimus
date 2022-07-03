package playGround

import (
	"fmt"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
)

type WindowSurvey struct {
	logger log.Logger
}

// NewInitializeSurvey initializes init survey
func NewwindowSurvey(logger log.Logger) *WindowSurvey {
	return &WindowSurvey{
		logger: logger,
	}
}

func (w *WindowSurvey) getWindowSize() int {
	var windowSize int
	for {
		err := survey.AskOne(&survey.Input{Message: "enter the window size in hours int the range [1,720]"},
			&windowSize,
		)
		if err != nil {
			fmt.Println(err)
		} else if windowSize >= 1 && windowSize <= 720 {
			return windowSize
		} else {
			fmt.Println("please enter a number in the valid range [1 , 720]")
		}
	}
}
func (w *WindowSurvey) getOffsetSize() int {
	var windowSize int
	for {
		err := survey.AskOne(&survey.Input{Message: "enter the OffSet in hours int the range [1,720]"},
			&windowSize,
		)
		if err != nil {
			fmt.Println(err)
		} else if (windowSize >= -720) && (windowSize <= 720) {
			return windowSize
		} else {
			fmt.Println("please enter a number in the valid range [ -720 , 720]")
		}
	}
}
func (w *WindowSurvey) getTrucatedTo() string {
	prompt := &survey.Select{
		Message: "Select the trucation paramterer",
		Options: []string{
			"hour",
			"day",
			"week",
			"month",
		},
		Default: "hour",
	}
	var dataStoreType string
	if err := survey.AskOne(prompt, &dataStoreType); err != nil {
		return dataStoreType
	}
	return dataStoreType
}
func (w *WindowSurvey) getSechduleDate() time.Time {
	var sechduledDate string
	for {
		prompt := &survey.Input{
			Message: "Enter the Sechduled Date in the format of YYYY-MM-DD HH:DD",
		}
		err := survey.AskOne(prompt, &sechduledDate)
		if err != nil {
			fmt.Println(err)
		} else {
			SechduledDate, errr := time.Parse("2006-01-02 15:04", sechduledDate)
			if errr != nil {
				fmt.Println(errr)
				fmt.Println("please enter in the specified format")
			} else {
				return SechduledDate
			}
		}
	}
}
