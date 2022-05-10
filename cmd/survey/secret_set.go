package survey

import "github.com/AlecAivazis/survey/v2"

// SecretSetSurvey defines survey for setting secret
type SecretSetSurvey struct{}

// NewSecretSetSurvey initializes survey to set secret
func NewSecretSetSurvey() *SecretSetSurvey {
	return &SecretSetSurvey{}
}

// AskToConfirmUpdate asks the user to confirm updating secret
func (s *SecretSetSurvey) AskToConfirmUpdate() (bool, error) {
	proceedWithUpdate := answerYes
	if err := survey.AskOne(&survey.Select{
		Message: "Secret already exists, proceed with update?",
		Options: []string{answerYes, answerNo},
		Default: answerNo,
	}, &proceedWithUpdate); err != nil {
		return false, err
	}
	if proceedWithUpdate == answerYes {
		return true, nil
	}
	return false, nil
}
