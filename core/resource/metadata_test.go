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

func TestStatus(t *testing.T) {
	t.Run("returns unknown on invalid status", func(t *testing.T) {
		status := resource.FromStringToStatus("invalid")
		assert.Equal(t, status.String(), "unknown")
	})
	t.Run("returns correct status", func(t *testing.T) {
		statuses := []string{
			"to_create", "to_update", "create_failure", "update_failure", "success",
			"unknown", "validation_failure", "validation_success", "skipped", "exist_in_store",
		}
		for _, typ := range statuses {
			status := resource.FromStringToStatus(typ)
			assert.Equal(t, status.String(), typ)
		}
	})
	t.Run("StatusForToCreate", func(t *testing.T) {
		t.Run("returns true when states is to_create or create_failure", func(t *testing.T) {
			statuses := []resource.Status{resource.StatusToCreate, resource.StatusCreateFailure}
			for _, status := range statuses {
				isForCreate := resource.StatusForToCreate(status)
				assert.True(t, isForCreate)
			}
		})
		t.Run("returns false on other statuses", func(t *testing.T) {
			statuses := []resource.Status{resource.StatusUnknown, resource.StatusValidationFailure, resource.StatusToUpdate}
			for _, status := range statuses {
				isForCreate := resource.StatusForToCreate(status)
				assert.False(t, isForCreate)
			}
		})
	})
	t.Run("StatusForToUpdate", func(t *testing.T) {
		t.Run("returns true when one of allowed status", func(t *testing.T) {
			statuses := []resource.Status{resource.StatusSuccess, resource.StatusToUpdate, resource.StatusUpdateFailure, resource.StatusExistInStore}
			for _, status := range statuses {
				isForUpdate := resource.StatusForToUpdate(status)
				assert.True(t, isForUpdate)
			}
		})
		t.Run("returns false on other statuses", func(t *testing.T) {
			statuses := []resource.Status{resource.StatusUnknown, resource.StatusValidationFailure, resource.StatusToCreate}
			for _, status := range statuses {
				isForUpdate := resource.StatusForToUpdate(status)
				assert.False(t, isForUpdate)
			}
		})
	})
	t.Run("StatusIsSuccess", func(t *testing.T) {
		t.Run("returns true when status is success", func(t *testing.T) {
			isSuccess := resource.StatusIsSuccess(resource.StatusSuccess)
			assert.True(t, isSuccess)
		})
		t.Run("returns false when other status", func(t *testing.T) {
			isSuccess := resource.StatusIsSuccess(resource.StatusValidationSuccess)
			assert.False(t, isSuccess)
		})
	})
}

func TestFieldValidate(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
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
