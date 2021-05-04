package local_test

import (
	"testing"

	"github.com/odpf/optimus/mock"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
	"github.com/odpf/optimus/store/local"
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

		bq2bqTrasnformer := new(mock.Transformer)
		bq2bqTrasnformer.On("Name").Return("bq2bq")

		allTasksRepo := new(mock.SupportedTransformationRepo)
		allTasksRepo.On("GetByName", "bq2bq").Return(bq2bqTrasnformer, nil)

		adapter := local.NewJobSpecAdapter(allTasksRepo, nil)
		modelJob, err := adapter.ToSpec(localJobParsed)
		assert.Nil(t, err)

		localJobBack, err := adapter.FromSpec(modelJob)
		assert.Nil(t, err)

		assert.Equal(t, localJobParsed, localJobBack)
	})
}
