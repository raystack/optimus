package local_test

import (
	"context"
	"testing"

	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/mock"

	"github.com/odpf/optimus/store/local"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestSpecAdapter(t *testing.T) {
	t.Run("should convert job with task from yaml to optimus model & back successfully", func(t *testing.T) {
		yamlSpec := `
version: 1
name: test_job
owner: test@example.com
schedule:
  start_date: "2021-02-03"
  interval: 0 2 * * *
behavior:
  depends_on_past: true
  catch_up: false
task:
  name: bq2bq
  config:
    PROJECT: project
    DATASET: dataset
    TABLE: table
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 168h
    offset: 0
    truncate_to: w
labels:
  orchestrator: optimus
dependencies: []
hooks: []
`
		var localJobParsed local.Job
		err := yaml.Unmarshal([]byte(yamlSpec), &localJobParsed)
		assert.Nil(t, err)

		bq2bqTrasnformer := new(mock.TaskPlugin)
		bq2bqTrasnformer.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
			Name: "bq2bq",
		}, nil)

		allTasksRepo := new(mock.SupportedTaskRepo)
		allTasksRepo.On("GetByName", "bq2bq").Return(bq2bqTrasnformer, nil)

		adapter := local.NewJobSpecAdapter(allTasksRepo, nil)
		modelJob, err := adapter.ToSpec(localJobParsed)
		assert.Nil(t, err)

		localJobBack, err := adapter.FromSpec(modelJob)
		assert.Nil(t, err)

		assert.Equal(t, localJobParsed, localJobBack)
	})
}
