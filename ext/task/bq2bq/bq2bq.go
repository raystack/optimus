package bq2bq

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/models"
)

var (
	validateName = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_\-]+$`, `invalid name (can only contain characters A-Z (in either case), 0-9, "-" or "_")`),
		survey.MinLength(3),
	)

	// a big query table can only contain the the characters [a-zA-Z0-9_].
	// https://cloud.google.com/bigquery/docs/tables
	validateTableName = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_-]+$`, "invalid table name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)"),
		survey.MaxLength(1024),
		survey.MinLength(3),
	)
)

type BQ2BQ struct {
}

func (b *BQ2BQ) GetName() string {
	return "bb_bq2bq"
}

func (b *BQ2BQ) GetDescription() string {
	return "BigQuery to BigQuery transformation task"
}

func (b *BQ2BQ) GetImage() string {
	return "asia.gcr.io/godata-platform/bumblebee:latest"
}

func (b *BQ2BQ) GetConfig() map[string]string {
	return map[string]string{
		"project":     "{{.Project}}",
		"dataset":     "{{.Dataset}}",
		"table":       "{{.Table}}",
		"load_method": "{{.LoadMethod}}",
		"sql_type":    "STANDARD",
	}
}

func (b *BQ2BQ) GetAssets() map[string]string {
	return map[string]string{
		"query.sql": `Select * from 1`,
	}
}

func (b *BQ2BQ) GetQuestions() []*survey.Question {
	return []*survey.Question{
		{
			Name:     "Project",
			Prompt:   &survey.Input{Message: "Project ID:"},
			Validate: validateName,
		},
		{
			Name:     "Dataset",
			Prompt:   &survey.Input{Message: "Dataset Name:"},
			Validate: validateName,
		},
		{
			Name:     "Table",
			Prompt:   &survey.Input{Message: "Table Name:"},
			Validate: validateTableName,
		},
		{
			Name: "LoadMethod",
			Prompt: &survey.Select{
				Message: "Load method to use on destination?",
				Options: []string{"REPLACE", "APPEND", "MERGE"},
				Default: "MERGE",
			},
			Validate: survey.Required,
		},
	}
}

func init() {
	models.SupportedTasks.Add(&BQ2BQ{})
}
