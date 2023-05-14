package bigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestRelationalExternalTable(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("fails validation when schema is invalid", func(t *testing.T) {
			et := bigquery.ExternalTable{
				Name:        "t-optimus.playground.test-sheet",
				Description: "",
				Schema: bigquery.Schema{{
					Name: "", Type: "table", Mode: "nullable",
				}},
			}
			err := et.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error in schema for t-optimus.playground.test-sheet")
		})
		t.Run("fails validation when source is invalid", func(t *testing.T) {
			et := bigquery.ExternalTable{
				Name:        "t-optimus.playground.test-sheet",
				Description: "",
				Schema: bigquery.Schema{
					{Name: "id", Type: "string", Mode: "nullable"},
				},
				Source: &bigquery.ExternalSource{SourceType: ""},
			}
			err := et.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "error in source for t-optimus.playground.test-sheet")
		})
	})
	t.Run("passes validations for with empty schema", func(t *testing.T) {
		et := bigquery.ExternalTable{
			Name:        "t-optimus.playground.test-sheet",
			Description: "",
			Source: &bigquery.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{"https://google.com/sheets"},
				Config:     nil,
			},
		}
		err := et.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground.test-sheet", et.FullName())
	})
	t.Run("passes validations for valid configuration", func(t *testing.T) {
		et := bigquery.ExternalTable{
			Name:        "t-optimus.playground.test-sheet",
			Description: "",
			Schema: bigquery.Schema{
				{Name: "id", Type: "string", Mode: "nullable"},
			},
			Source: &bigquery.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{"https://google.com/sheets"},
				Config:     nil,
			},
		}
		err := et.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground.test-sheet", et.FullName())
	})
}

func TestExternalSourceValidate(t *testing.T) {
	t.Run("when valid", func(t *testing.T) {
		t.Run("returns error on source type", func(t *testing.T) {
			es := bigquery.ExternalSource{
				SourceType: "",
				SourceURIs: []string{},
				Config:     nil,
			}

			err := es.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "source type is empty")
		})
		t.Run("returns error when uri list is empty", func(t *testing.T) {
			es := bigquery.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{},
				Config:     nil,
			}

			err := es.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "source uri list is empty")
		})
		t.Run("returns error when uri is invalid", func(t *testing.T) {
			es := bigquery.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{""},
				Config:     nil,
			}

			err := es.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "uri is empty")
		})
	})
	t.Run("returns no error when valid", func(t *testing.T) {
		es := bigquery.ExternalSource{
			SourceType: "GOOGLE_SHEETS",
			SourceURIs: []string{"https://google.com/sheets"},
			Config:     nil,
		}

		err := es.Validate()
		assert.Nil(t, err)
	})
}
