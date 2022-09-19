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
		assert.Equal(t, "not found for entity project: project t-optimus not found", de.Error())
	})
	t.Run("creates domain error from store error", func(t *testing.T) {
		de := service.FromError(store.ErrResourceNotFound, "namespace", "invalid name")

		assert.Error(t, de)
		assert.Equal(t, "not found for entity namespace: invalid name, resource not found", de.Error())
	})
	t.Run("creates domain error and returns error message", func(t *testing.T) {
		err1 := errors.New("random database error")
		de1 := service.FromError(err1, "project", "not able to get project")
		de2 := service.FromError(de1, "namespace", "error while fetching namespace")

		assert.Error(t, de2)
		expectedErrorString := "internal error for namespace: error while fetching namespace, internal error (internal error for project: not able to get project, internal error (random database error))"
		assert.Equal(t, expectedErrorString, de2.DebugString())
	})
	t.Run("creates debug string when err inside is nil", func(t *testing.T) {
		de1 := service.NewError("project", service.ErrInternalError, "some error with project")
		de2 := service.FromError(de1, "namespace", "error while fetching namespace")

		assert.Error(t, de2)
		expectedErrorString := "internal error for namespace: error while fetching namespace, internal error (internal error for project: some error with project ())"
		assert.Equal(t, expectedErrorString, de2.DebugString())
	})
	t.Run("allows unwrapping of error in error chain", func(t *testing.T) {
		err1 := store.ErrResourceNotFound
		de2 := service.FromError(err1, "namespace", "error while fetching namespace")

		assert.Error(t, de2)
		assert.ErrorIs(t, de2, store.ErrResourceNotFound)

		expectedErrorString := "not found for entity namespace: error while fetching namespace, resource not found"
		assert.Equal(t, expectedErrorString, de2.Error())
	})
}
