package survey

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/salt/log"
)

// BackupCreateSurvey defines survey for creating backup
type BackupCreateSurvey struct {
	logger log.Logger
}

// NewBackupCreateSurvey initializes surveys for creating backup
func NewBackupCreateSurvey(logger log.Logger) *BackupCreateSurvey {
	return &BackupCreateSurvey{
		logger: logger,
	}
}

// AskResourceName asks the user to add resource name for creating backup
func (b *BackupCreateSurvey) AskResourceName() (string, error) {
	var resourceName string
	return resourceName, survey.AskOne(
		&survey.Input{
			Message: "What is the resource name?",
			Help:    "Input urn of the resource",
		},
		&resourceName,
		survey.WithValidator(
			survey.ComposeValidators(
				validateNoSlash,
				survey.MinLength(3),
				survey.MaxLength(1024),
			),
		),
	)
}

// AskBackupDescription asks the user the need of backup creation
func (b *BackupCreateSurvey) AskBackupDescription() (string, error) {
	var description string
	return description, survey.AskOne(
		&survey.Input{
			Message: "Why is this backup needed?",
			Help:    "Describe intention to help identify the backup",
		},
		&description,
		survey.WithValidator(
			survey.ComposeValidators(survey.MinLength(3)),
		),
	)
}

// AskConfirmToContinue asks the user to confirm whether to continue on creating backup or not
func (b *BackupCreateSurvey) AskConfirmToContinue() (bool, error) {
	proceedWithBackup := answerYes
	if err := survey.AskOne(&survey.Select{
		Message: "Proceed with backup?",
		Options: []string{answerYes, answerNo},
		Default: answerNo,
	}, &proceedWithBackup); err != nil {
		return false, err
	}
	if proceedWithBackup == answerNo {
		b.logger.Info(logger.ColoredNotice("Aborting..."))
		return false, nil
	}
	return true, nil
}
