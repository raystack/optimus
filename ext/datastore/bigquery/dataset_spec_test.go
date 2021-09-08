package bigquery

import (
	"testing"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestDatasetSpecHandler(t *testing.T) {
	t.Run("should convert from and to yaml successfully", func(t *testing.T) {
		fl := `
version: 1
name: prj.datas
type: table
spec:
  description: hello-world
labels:
  key: value
`
		handler := datasetSpecHandler{}
		res, err := handler.FromYaml([]byte(fl))
		assert.Nil(t, err)
		converted, err := handler.ToYaml(res)
		assert.Nil(t, err)
		resBack, err := handler.FromYaml(converted)
		assert.Nil(t, err)
		assert.Equal(t, res, resBack)
	})
	t.Run("should convert from and to proto successfully", func(t *testing.T) {
		originalRes := models.ResourceSpec{
			Version:   1,
			Name:      "proj.datas",
			Type:      "table",
			Datastore: This,
			Spec: BQDataset{
				Project: "proj",
				Dataset: "datas",
				Metadata: BQDatasetMetadata{
					Description: "test table",
				},
			},
			Assets: map[string]string{
				"view.sql": ("-- some sql query"),
			},
			Labels: map[string]string{
				"key": "val",
			},
		}
		handler := datasetSpecHandler{}
		protoInBytes, err := handler.ToProtobuf(originalRes)
		assert.Nil(t, err)
		resBack, err := handler.FromProtobuf(protoInBytes)
		assert.Nil(t, err)
		assert.Equal(t, originalRes, resBack)
	})
}
