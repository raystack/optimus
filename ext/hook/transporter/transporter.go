package transporter

import (
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/iancoleman/strcase"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

// Transporter pushes BQ data to Kafka
// required configs:
// - TRANSPORTER_KAFKA_BROKERS
// - TRANSPORTER_STENCIL_HOST e.g. http://odpf/artifactory/proto-descriptors/ocean-proton/latest
type Transporter struct {
}

func (t *Transporter) GetName() string {
	return "transporter"
}

func (t *Transporter) GetImage() string {
	return "odpf/optimus-task-transporter:latest"
}

func (t *Transporter) GetDescription() string {
	return "BigQuery to Kafka Transformer"
}

func (t *Transporter) GetType() models.HookType {
	return models.HookTypePost
}

func (t *Transporter) AskQuestions(_ models.UnitOptions) (map[string]interface{}, error) {
	questions := []*survey.Question{
		{
			Name: "FilterExpression",
			Prompt: &survey.Input{
				Message: "Filter expression for extracting transformation rows?",
				Help:    "for example: event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'",
				Default: "",
			},
		},
	}
	inputsRaw := make(map[string]interface{})
	if err := survey.Ask(questions, &inputsRaw); err != nil {
		return nil, err
	}
	return inputsRaw, nil
}

func (t *Transporter) GenerateConfig(hookInputs map[string]interface{}, jobUnitData models.UnitData) (models.JobSpecConfigs, error) {
	project, ok1 := jobUnitData.Config.Get("PROJECT")
	dataset, ok2 := jobUnitData.Config.Get("DATASET")
	table, ok3 := jobUnitData.Config.Get("TABLE")
	filterExp, ok4 := hookInputs["FilterExpression"]
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nil, errors.New("missing config key required to generate configuration")
	}
	return models.JobSpecConfigs{
		{
			Name:  "KAFKA_TOPIC",
			Value: getKafkaTopicName(project, dataset, table),
		},
		{
			Name:  "PROTO_SCHEMA",
			Value: getProtoSchemaForBQTable(project, dataset, table),
		},
		{
			Name:  "STENCIL_URL",
			Value: `{{.GLOBAL__TRANSPORTER_STENCIL_HOST}}`,
		},
		{
			Name:  "FILTER_EXPRESSION",
			Value: filterExp.(string),
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
		{
			Name:  "PRODUCER_CONFIG_BOOTSTRAP_SERVERS",
			Value: `{{.GLOBAL__TRANSPORTER_KAFKA_BROKERS}}`,
		},
	}, nil
}

func getProtoSchemaForBQTable(project, dataset, table string) string {
	return fmt.Sprintf(
		"%s.%s.%s",
		convertToProtoSchemaNamingConvention(project),
		convertToProtoSchemaNamingConvention(dataset),
		strcase.ToCamel(table),
	)
}

func convertToProtoSchemaNamingConvention(input string) string {
	return strings.ReplaceAll(input, "-", "_")
}

func getKafkaTopicName(project, dataset, table string) string {
	topicName := fmt.Sprintf("optimus_%s-%s-%s", project, dataset, table)
	if len(topicName) >= 255 {
		topicName = topicName[:254]
	}
	return topicName
}

func (t *Transporter) GetDependsOn() []string {
	return []string{"predator"}
}

func init() {
	if err := models.HookRegistry.Add(&Transporter{}); err != nil {
		panic(err)
	}
}
