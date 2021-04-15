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
// - TRANSPORTER_STENCIL_HOST
type Transporter struct {
}

func (t *Transporter) Name() string {
	return "transporter"
}

func (t *Transporter) Image() string {
	return "odpf/optimus-task-transporter:latest"
}

func (t *Transporter) Description() string {
	return "BigQuery to Kafka Transformer"
}

func (t *Transporter) Type() models.HookType {
	return models.HookTypePost
}

func (t *Transporter) AskQuestions(_ models.AskQuestionRequest) (models.AskQuestionResponse, error) {
	questions := []*survey.Question{
		{
			Name: "FilterExpression",
			Prompt: &survey.Input{
				Message: "Filter expression for extracting transformation rows?",
				Help:    `for example: DATE(event_timestamp) >= "{{ .DSTART|Date }}" AND DATE(event_timestamp) < "{{ .DEND|Date }}"`,
				Default: "",
			},
		},
	}
	inputsRaw := make(map[string]interface{})
	if err := survey.Ask(questions, &inputsRaw); err != nil {
		return models.AskQuestionResponse{}, err
	}
	return models.AskQuestionResponse{Answers: inputsRaw}, nil
}

func (t *Transporter) GenerateConfig(request models.GenerateConfigWithTaskRequest) (models.GenerateConfigResponse, error) {
	project, ok1 := request.TaskConfig.Get("PROJECT")
	dataset, ok2 := request.TaskConfig.Get("DATASET")
	table, ok3 := request.TaskConfig.Get("TABLE")
	filterExp, ok4 := request.Inputs["FilterExpression"]
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return models.GenerateConfigResponse{}, errors.New("missing config key required to generate configuration")
	}
	return models.GenerateConfigResponse{
		Config: models.JobSpecConfigs{
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

func (t *Transporter) DependsOn() []string {
	return []string{"predator"}
}

func init() {
	if err := models.HookRegistry.Add(&Transporter{}); err != nil {
		panic(err)
	}
}
