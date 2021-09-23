package bigquery

import (
	"testing"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestTableSpecHandler(t *testing.T) {
	t.Run("should convert from and to yaml successfully", func(t *testing.T) {
		fl := `
version: 1
name: prj.datas.t1
type: table
spec:
  description: hello-world
  schema:
  - name: aa
    type: INT
  partition:
    field: aa
    expiration: 24
`
		tabHandler := tableSpecHandler{}
		res, err := tabHandler.FromYaml([]byte(fl))
		assert.Nil(t, err)
		converted, err := tabHandler.ToYaml(res)
		assert.Nil(t, err)
		resBack, err := tabHandler.FromYaml(converted)
		assert.Nil(t, err)
		assert.Equal(t, res, resBack)
	})

	t.Run("should convert from and to proto successfully", func(t *testing.T) {
		originalRes := models.ResourceSpec{
			Version:   1,
			Name:      "proj.datas.tab",
			Type:      "table",
			Datastore: This,
			Spec: BQTable{
				Project: "proj",
				Dataset: "datas",
				Table:   "tab",
				Metadata: BQTableMetadata{
					Description: "test table",
					Schema: BQSchema{
						{
							Name:        "col1",
							Type:        "INT",
							Description: "desc",
							Schema:      nil,
						},
					},
					Cluster: &BQClusteringInfo{
						Using: []string{"col1"},
					},
					Source: &BQExternalSource{
						SourceType: string(ExternalTableTypeGoogleSheets),
						SourceURIs: []string{"http://googlesheets.com/1234"},
						Config:     map[string]interface{}{"skip_leading_rows": 1.0, "range": "A!:A1:B1"},
					},
				},
			},
			Assets: map[string]string{
				"view.sql": ("-- some sql query"),
			},
			Labels: map[string]string{
				"key": "val",
			},
		}
		s := tableSpecHandler{}
		protoInBytes, err := s.ToProtobuf(originalRes)
		assert.Nil(t, err)
		resBack, err := s.FromProtobuf(protoInBytes)
		assert.Nil(t, err)
		assert.Equal(t, originalRes, resBack)
	})

	t.Run("should generate urn successfully", func(t *testing.T) {
		project := "sample-project"
		dataset := "sample-dataset"
		table := "sample-table"

		urn, err := tableSpec{}.GenerateURN(BQTable{
			Project: project,
			Dataset: dataset,
			Table:   table,
		})

		assert.Nil(t, err)
		assert.Equal(t, "bigquery://sample-project:sample-dataset.sample-table", urn)
	})
}
