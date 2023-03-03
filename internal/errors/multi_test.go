package errors_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/errors"
)

func TestMultiError(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		t.Run("does not add nil error", func(t *testing.T) {
			me := errors.NewMultiError("multi")
			me.Append(nil)

			assert.Nil(t, errors.MultiToError(me))
		})
		t.Run("adds other type of error", func(t *testing.T) {
			me := errors.NewMultiError("multi")
			me.Append(errors.NotFound("dummy", "not found"))

			assert.NotNil(t, errors.MultiToError(me))
			assert.EqualError(t, me, "multi:\n not found for entity dummy: not found")
		})
		t.Run("adds errors in multi error to list", func(t *testing.T) {
			me1 := errors.NewMultiError("multi1")
			me1.Append(errors.NotFound("dummy", "not found"))

			me := errors.NewMultiError("top level error")
			me.Append(me1)

			assert.NotNil(t, errors.MultiToError(me))
			assert.EqualError(t, me, "top level error:\n not found for entity dummy: not found")
		})
	})

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
