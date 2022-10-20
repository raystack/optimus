package errors_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/errors"
)

func TestMultiError(t *testing.T) {
	t.Run("returns nil", func(t *testing.T) {
		t.Run("when error is nil", func(t *testing.T) {
			var e error

			rst := errors.MultiToError(e)
			assert.Nil(t, rst)
		})

		t.Run("when error is empty multi error", func(t *testing.T) {
			e := errors.NewMultiError("multiple errors encountered")

			rst := errors.MultiToError(e)
			assert.Nil(t, rst)
		})
	})

	t.Run("returns not nil", func(t *testing.T) {
		t.Run("when error is not multi error", func(t *testing.T) {
			e := errors.NotFound("entity", "error message")

			rst := errors.MultiToError(e)
			assert.NotNil(t, rst)
		})

		t.Run("when error is non-empty multi error", func(t *testing.T) {
			e := errors.NewMultiError("multiple errors encountered")
			e.Append(errors.NotFound("entity", "data not found"))

			rst := errors.MultiToError(e)
			assert.NotNil(t, rst)
		})
	})
}
