package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestKind(t *testing.T) {
	t.Run("returns error on invalid type", func(t *testing.T) {
		_, err := resource.FromStringToKind("invalid")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: unknown kind invalid")
	})
	t.Run("returns correct kind", func(t *testing.T) {
		types := []string{
			"table", "view", "external_table", "dataset",
		}
		for _, typ := range types {
			kind, err := resource.FromStringToKind(typ)
			assert.Nil(t, err)
			assert.Equal(t, typ, kind.String())
		}
	})
}

func TestFieldValidate(t *testing.T) {
	t.Run("returns error when name is empty", func(t *testing.T) {
		f := resource.Field{
			Name:   "",
			Type:   "string",
			Mode:   "nullable",
			Schema: nil,
		}

		err := f.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_schema: field name is empty")
	})
	t.Run("returns error when type is empty", func(t *testing.T) {
		f := resource.Field{
			Name:   "name",
			Type:   "",
			Mode:   "nullable",
			Schema: nil,
		}

		err := f.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_schema: field type is empty for name")
	})
	t.Run("returns error when mode is incorrect", func(t *testing.T) {
		f := resource.Field{
			Name:   "name",
			Type:   "string",
			Mode:   "invalid",
			Schema: nil,
		}

		err := f.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_schema: unknown "+
			"field mode invalid for name")
	})
	t.Run("returns error when schema is invalid", func(t *testing.T) {
		f := resource.Field{
			Name: "name",
			Type: "string",
			Mode: "nullable",
			Schema: []resource.Field{{
				Name: "",
				Type: "string",
			}},
		}

		err := f.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_schema: field name is empty")
	})
	t.Run("return no error when valid", func(t *testing.T) {
		f := resource.Field{
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
		schema := resource.Schema{
			{
				Name:   "name1",
				Type:   "",
				Mode:   "nullable",
				Schema: nil,
			},
		}
		err := schema.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_schema: field type is empty for name1")
	})
	t.Run("return no error when valid", func(t *testing.T) {
		schema := resource.Schema{
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
