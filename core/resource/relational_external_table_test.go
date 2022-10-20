package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestRelationalExternalTable(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("fails validation when schema is empty", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.Bigquery, "t-optimus", "playground")
			assert.Nil(t, err)

			et := resource.ExternalTable{
				Name:        "test-sheet",
				Dataset:     ds,
				Description: "",
				Schema:      nil,
			}
			err = et.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: invalid schema "+
				"for t-optimus.playground.test-sheet")
		})
		t.Run("fails validation when schema is invalid", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.Bigquery, "t-optimus", "playground")
			assert.Nil(t, err)

			et := resource.ExternalTable{
				Name:        "test-sheet",
				Dataset:     ds,
				Description: "",
				Schema: resource.Schema{{
					Name: "", Type: "table", Mode: "nullable"}},
			}
			err = et.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: error in schema "+
				"for t-optimus.playground.test-sheet")
		})
		t.Run("fails validation when source is invalid", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.Bigquery, "t-optimus", "playground")
			assert.Nil(t, err)

			et := resource.ExternalTable{
				Name:        "test-sheet",
				Dataset:     ds,
				Description: "",
				Schema: resource.Schema{
					{Name: "id", Type: "string", Mode: "nullable"},
				},
				Source: &resource.ExternalSource{SourceType: ""},
			}
			err = et.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: error in source "+
				"for t-optimus.playground.test-sheet")
		})
	})
	t.Run("passes validations for valid configuration", func(t *testing.T) {
		ds, err := resource.DataSetFrom(resource.Bigquery, "t-optimus", "playground")
		assert.Nil(t, err)

		et := resource.ExternalTable{
			Name:        "test-sheet",
			Dataset:     ds,
			Description: "",
			Schema: resource.Schema{
				{Name: "id", Type: "string", Mode: "nullable"},
			},
			Source: &resource.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{"https://google.com/sheets"},
				Config:     nil,
			},
		}
		err = et.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "bigquery://t-optimus:playground.test-sheet", et.URN())
	})
}

func TestExternalSourceValidate(t *testing.T) {
	t.Run("when valid", func(t *testing.T) {
		t.Run("returns error on source type", func(t *testing.T) {
			es := resource.ExternalSource{
				SourceType: "",
				SourceURIs: []string{},
				Config:     nil,
			}

			err := es.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: source type is empty")
		})
		t.Run("returns error when uri list is empty", func(t *testing.T) {
			es := resource.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{},
				Config:     nil,
			}

			err := es.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: source uri list is empty")
		})
		t.Run("returns error when uri is invalid", func(t *testing.T) {
			es := resource.ExternalSource{
				SourceType: "GOOGLE_SHEETS",
				SourceURIs: []string{""},
				Config:     nil,
			}

			err := es.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: uri is empty")
		})
	})
	t.Run("returns no error when valid", func(t *testing.T) {
		es := resource.ExternalSource{
			SourceType: "GOOGLE_SHEETS",
			SourceURIs: []string{"https://google.com/sheets"},
			Config:     nil,
		}

		err := es.Validate()
		assert.Nil(t, err)
	})
}
