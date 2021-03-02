package transporter

import (
	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/models"
)

// Predator profiles and audits BQ tables
// required configs
// - PREDATOR_HOST, 
type Predator struct {
}

func (t *Predator) GetName() string {
	return "predator"
}

func (t *Predator) GetImage() string {
	return "odpf/optimus-task-predator:latest"
}

func (t *Predator) GetDescription() string {
	return "Auditing and Profiling Tool for BigQuery"
}

func (t *Predator) GetType() models.HookType {
	return models.HookTypePost
}

func (t *Predator) GetQuestions() []*survey.Question {
	return []*survey.Question{
		{
			Name: "FilterExpression",
			Prompt: &survey.Input{
				Message: "Filter expression for extracting transformation rows?",
				Help:    "for example: event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'",
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
}

func (t *Predator) GetConfig(_ models.UnitData) (map[string]string, error) {
	return map[string]string{
		"AUDIT_TIME":   `{{ "{{.EXECUTION_TIME}}" }}`,
		"FILTER":       "{{.FilterExpression}}",
		"GROUP":        "{{.Group}}",
		"MODE":         "{{.Mode}}",
		"PREDATOR_URL": `{{ "{{.GLOBAL__PREDATOR_HOST}}" }}`,
		"SUB_COMMAND":  "profile_audit",
		"BQ_PROJECT":   `{{ "{{.TASK__PROJECT}}" }}`,
		"BQ_DATASET":   `{{ "{{.TASK__DATASET}}" }}`,
		"BQ_TABLE":     `{{ "{{.TASK__TABLE}}" }}`,
	}, nil
}

func (t *Predator) GetDependsOn() []string {
	return []string{}
}

func init() {
	if err := models.HookRegistry.Add(&Predator{}); err != nil {
		panic(err)
	}
}
