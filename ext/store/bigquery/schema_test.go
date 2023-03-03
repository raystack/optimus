package bigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/bigquery"
)

func TestFieldValidate(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			f := bigquery.Field{
				Name:   "",
				Type:   "string",
				Mode:   "nullable",
				Schema: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field name is empty")
		})
		t.Run("returns error when type is empty", func(t *testing.T) {
			f := bigquery.Field{
				Name:   "name",
				Type:   "",
				Mode:   "nullable",
				Schema: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field type is empty for name")
		})
		t.Run("returns error when mode is incorrect", func(t *testing.T) {
			f := bigquery.Field{
				Name:   "name",
				Type:   "string",
				Mode:   "invalid",
				Schema: nil,
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "unknown field mode invalid for name")
		})
		t.Run("returns error when schema is invalid", func(t *testing.T) {
			f := bigquery.Field{
				Name: "name",
				Type: "string",
				Mode: "nullable",
				Schema: []bigquery.Field{{
					Name: "",
					Type: "string",
				}},
			}

			err := f.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "field name is empty")
		})
	})
	t.Run("return no error when valid", func(t *testing.T) {
		f := bigquery.Field{
			Name:   "name",
			Type:   "string",
			Mode:   "nullable",
			Schema: nil,
		}

		err := f.Validate()
		assert.Nil(t, err)
	})
}

func TestSchemaValidate(t *testing.T) {
	t.Run("returns error when error in one of schema field", func(t *testing.T) {
		schema := bigquery.Schema{
			{
				Name:   "name1",
				Type:   "",
				Mode:   "nullable",
				Schema: nil,
			},
		}
		err := schema.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "field type is empty for name1")
	})
	t.Run("return no error when valid", func(t *testing.T) {
		schema := bigquery.Schema{
			{
				Name:   "name1",
				Type:   "string",
				Mode:   "nullable",
				Schema: nil,
			},
			{
				Name:   "name2",
				Type:   "string",
				Mode:   "nullable",
				Schema: nil,
			},
		}
		err := schema.Validate()
		assert.Nil(t, err)
	})
}
