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

func (t *Transporter) GetQuestions() []*survey.Question {
	return []*survey.Question{
		{
			Name: "FilterExpression",
			Prompt: &survey.Input{
				Message: "Filter expression for extracting transformation rows?",
				Help:    "for example: event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'",
			},
		},
	}
}

func (t *Transporter) GetConfig(jobUnitData models.UnitData) (map[string]string, error) {
	project, ok1 := jobUnitData.Config["PROJECT"]
	dataset, ok2 := jobUnitData.Config["DATASET"]
	table, ok3 := jobUnitData.Config["TABLE"]
	if !ok1 || !ok2 || !ok3 {
		return nil, errors.New("missing config key required to generate configuration")
	}

	return map[string]string{
		"KAFKA_TOPIC":                       getKafkaTopicName(project, dataset, table),
		"PROTO_SCHEMA":                      getProtoSchemaForBQTable(project, dataset, table),
		"STENCIL_URL":                       `{{ "{{.GLOBAL__TRANSPORTER_STENCIL_HOST}}" }}`,
		"FILTER_EXPRESSION":                 "{{.FilterExpression}}",
		"BQ_PROJECT":                        `{{ "{{.TASK__PROJECT}}" }}`,
		"BQ_DATASET":                        `{{ "{{.TASK__DATASET}}" }}`,
		"BQ_TABLE":                          `{{ "{{.TASK__TABLE}}" }}`,
		"PRODUCER_CONFIG_BOOTSTRAP_SERVERS": `{{ "{{.GLOBAL__TRANSPORTER_KAFKA_BROKERS}}" }}`,
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
