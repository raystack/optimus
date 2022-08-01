package survey

import (
	"time"

	"github.com/AlecAivazis/survey/v2"
)

type dateSurvey struct {
}

// NewJobSurvey initializes job survey
func NewDateSurvey() *dateSurvey {
	return &dateSurvey{}
}
func (*dateSurvey) Get_month() (time.Month, error) {
	options := make([]string, 12)
	var i time.Month
	i = 1
	for i < 13 {
		options[i-1] = i.String()
		i++
	}
	prompt := &survey.Select{
		Message: "Please choose the month:",
		Options: options,
		Default: options[time.Now().Month()-1],
	}
	var response string
	if err := survey.AskOne(prompt, &response); err != nil {
		return 0, err
	}
	var month_map = make(map[string]time.Month)
	i = 1
	for i < 13 {
		month_map[i.String()] = i
		i++
	}
	return month_map[response], nil
}
