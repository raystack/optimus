package models_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/models"
)

func TestNewWindow(t *testing.T) {
	t.Run("should return window and nil if version is recognized", func(t *testing.T) {
		testCases := []struct {
			version    int
			truncateTo string
			offset     string
			size       string

			testMessage string
		}{
			{version: 1, truncateTo: "h", offset: "1h", size: "1h", testMessage: "version 1"},
			{version: 2, truncateTo: "h", offset: "1h", size: "1h", testMessage: "version 2"},
		}

		for _, tCase := range testCases {
			actualWindow, actualError := models.NewWindow(tCase.version, tCase.truncateTo, tCase.offset, tCase.size)

			assert.NotNil(t, actualWindow, tCase.testMessage)
			assert.NoError(t, actualError, tCase.testMessage)
		}
	})

	t.Run("should return nil and error if version is not recognized", func(t *testing.T) {
		version := 0
		truncateTo := "h"
		offset := "1h"
		size := "1h"

		actualWindow, actualError := models.NewWindow(version, truncateTo, offset, size)

		assert.Nil(t, actualWindow)
		assert.Error(t, actualError)
	})
}
