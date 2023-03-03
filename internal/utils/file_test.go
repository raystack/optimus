package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

func TestIsPathOccupied(t *testing.T) {
	t.Run("should return false and nil if path does not exist", func(t *testing.T) {
		path := "unknown"

		actualExistence, actualErr := utils.IsPathOccupied(path)

		assert.False(t, actualExistence)
		assert.NoError(t, actualErr)
	})

	t.Run("should return false and error if unknown error encountered", func(t *testing.T) {
		path := string([]byte{0})

		actualExistence, actualErr := utils.IsPathOccupied(path)

		assert.False(t, actualExistence)
		assert.Error(t, actualErr)
	})

	t.Run("should return true and nil if path exist", func(t *testing.T) {
		path := "file_test.go"

		actualExistence, actualErr := utils.IsPathOccupied(path)

		assert.True(t, actualExistence)
		assert.NoError(t, actualErr)
	})
}
