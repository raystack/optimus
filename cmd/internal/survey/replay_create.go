package survey

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
)

// ReplayCreateSurvey defines the survey for replay create
type ReplayCreateSurvey struct {
	logger log.Logger
}

// NewReplayCreateSurvey initializes survey for replay create
func NewReplayCreateSurvey(logger log.Logger) *ReplayCreateSurvey {
	return &ReplayCreateSurvey{
		logger: logger,
	}
}

// AskConfirmToContinue asks the user to confirm whether to continue on replay or not
func (r *ReplayCreateSurvey) AskConfirmToContinue() (bool, error) {
	proceedWithReplay := answerYes
	if err := survey.AskOne(&survey.Select{
		Message: "Proceed with replay?",
		Options: []string{answerYes, answerNo},
		Default: answerNo,
	}, &proceedWithReplay); err != nil {
		return false, err
	}
	if proceedWithReplay == answerNo {
		r.logger.Warn("Aborting...")
		return false, nil
	}
	return true, nil
}
