package service_test

import (
	"errors"
	"testing"

	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
	"github.com/stretchr/testify/assert"
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
		expectedErrorString := "error while fetching namespace: Internal Error for namespace: not able to get project: Internal Error for project: random database error"
		assert.Equal(t, expectedErrorString, de2.DebugString())
	})
}
