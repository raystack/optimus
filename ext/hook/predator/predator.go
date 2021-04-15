package transporter

import (
	"github.com/odpf/optimus/utils"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

// Predator profiles and audits BQ tables
// required configs
// - PREDATOR_HOST
type Predator struct {
}

func (t *Predator) Name() string {
	return "predator"
}

func (t *Predator) Image() string {
	return "odpf/optimus-task-predator:latest"
}

func (t *Predator) Description() string {
	return "Auditing and Profiling Tool for BigQuery"
}

func (t *Predator) Type() models.HookType {
	return models.HookTypePost
}

func (t *Predator) AskQuestions(_ models.AskQuestionRequest) (models.AskQuestionResponse, error) {
	questions := []*survey.Question{
		{
			Name: "FilterExpression",
			Prompt: &survey.Input{
				Message: "Filter expression for extracting transformation rows?",
				Help:    `for example: DATE(event_timestamp) >= "{{ .DSTART|Date }}" AND DATE(event_timestamp) < "{{ .DEND|Date }}"`,
				Default: "",
			},
			Validate: survey.MinLength(5),
		},
		{
			Name: "Group",
			Prompt: &survey.Input{
				Message: "Specify the profile/audit result grouping field (empty to not group the result)",
				Help:    "for example: __PARTITION__",
			},
		},
		{
			Name: "Mode",
			Prompt: &survey.Select{
				Message: "Choose the profiling mode",
				Options: []string{"complete", "incremental"},
				Default: "complete",
			},
		},
	}
	inputsRaw := make(map[string]interface{})
	if err := survey.Ask(questions, &inputsRaw); err != nil {
		return models.AskQuestionResponse{}, err
	}
	return models.AskQuestionResponse{Answers: inputsRaw}, nil
}

func (t *Predator) GenerateConfig(request models.GenerateConfigWithTaskRequest) (models.GenerateConfigResponse, error) {
	_, ok1 := request.Inputs["FilterExpression"]
	_, ok2 := request.Inputs["Group"]
	_, ok3 := request.Inputs["Mode"]
	if !ok1 || !ok2 || !ok3 {
		return models.GenerateConfigResponse{}, errors.New("missing config key required to generate configuration")
	}
	stringInputs, err := utils.ConvertToStringMap(request.Inputs)
	if err != nil {
		return models.GenerateConfigResponse{}, errors.Wrap(err, "unidentified config in hook")
	}
	return models.GenerateConfigResponse{
		Config: models.JobSpecConfigs{
			{
				Name:  "AUDIT_TIME",
				Value: `{{.EXECUTION_TIME}}`,
			},
			{
				Name:  "FILTER",
				Value: stringInputs["FilterExpression"],
			},
			{
				Name:  "GROUP",
				Value: stringInputs["Group"],
			},
			{
				Name:  "MODE",
				Value: stringInputs["Mode"],
			},
			{
				Name:  "PREDATOR_URL",
				Value: `{{.GLOBAL__PREDATOR_HOST}}`,
			},
			{
				Name:  "SUB_COMMAND",
				Value: "profile_audit",
			},
			{
				Name:  "BQ_PROJECT",
				Value: `{{.TASK__PROJECT}}`,
			},
			{
				Name:  "BQ_DATASET",
				Value: `{{.TASK__DATASET}}`,
			},
			{
				Name:  "BQ_TABLE",
				Value: `{{.TASK__TABLE}}`,
			},
		},
	}, nil
}

func (t *Predator) DependsOn() []string {
	return []string{}
}

func init() {
	if err := models.HookRegistry.Add(&Predator{}); err != nil {
		panic(err)
	}
}
