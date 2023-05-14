package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

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
