package service_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

func TestErrors(t *testing.T) {
	t.Run("creates domain error and returns error message", func(t *testing.T) {
		de := service.NewError("project", service.ErrNotFound, "project t-optimus not found")

		assert.Error(t, de)
		assert.Equal(t, "project t-optimus not found: not found for entity project", de.Error())
	})
	t.Run("creates domain error from store error", func(t *testing.T) {
		de := service.FromError(store.ErrResourceNotFound, "namespace", "invalid name")

		assert.Error(t, de)
		assert.Equal(t, "invalid name: not found for entity namespace", de.Error())
	})
	t.Run("creates domain error and returns error message", func(t *testing.T) {
		err1 := errors.New("random database error")
		de1 := service.FromError(err1, "project", "not able to get project")
		de2 := service.FromError(de1, "namespace", "error while fetching namespace")

		assert.Error(t, de2)
		expectedErrorString := "error while fetching namespace: internal error for namespace: not able to get project: internal error for project: random database error"
		assert.Equal(t, expectedErrorString, de2.DebugString())
	})
	t.Run("creates debug string when err inside is nil", func(t *testing.T) {
		de1 := service.NewError("project", service.ErrInternalError, "some error with project")
		de2 := service.FromError(de1, "namespace", "error while fetching namespace")

		assert.Error(t, de2)
		expectedErrorString := "error while fetching namespace: internal error for namespace: some error with project: internal error for project: "
		assert.Equal(t, expectedErrorString, de2.DebugString())
	})
	t.Run("allows unwrapping of error in error chain", func(t *testing.T) {
		err1 := store.ErrResourceNotFound
		de2 := service.FromError(err1, "namespace", "error while fetching namespace")

		assert.Error(t, de2)
		assert.ErrorIs(t, de2, store.ErrResourceNotFound)

		expectedErrorString := "error while fetching namespace: not found for entity namespace"
		assert.Equal(t, expectedErrorString, de2.Error())
	})
}
