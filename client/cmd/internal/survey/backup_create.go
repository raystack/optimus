package survey

import (
	"github.com/AlecAivazis/survey/v2"
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

// AskResourceNames asks the user to add resource name for creating backup
func (*BackupCreateSurvey) AskResourceNames() (string, error) {
	var resourceNames string
	if err := survey.AskOne(
		&survey.Input{
			Message: "What are the resource names ? (separated by ,)",
			Help:    "Input name of the resources",
		},
		&resourceNames,
		survey.WithValidator(
			survey.ComposeValidators(
				validateNoSlash,
				survey.MinLength(3),
			),
		),
	); err != nil {
		return "", err
	}
	return resourceNames, nil
}

// AskBackupDescription asks the user the need of backup creation
func (*BackupCreateSurvey) AskBackupDescription() (string, error) {
	var description string
	if err := survey.AskOne(
		&survey.Input{
			Message: "Why is this backup needed?",
			Help:    "Describe intention to help identify the backup",
		},
		&description,
		survey.WithValidator(
			survey.ComposeValidators(survey.MinLength(3)),
		),
	); err != nil {
		return "", err
	}
	return description, nil
}
