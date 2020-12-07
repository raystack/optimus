package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/utils"
)

func TestValidator(t *testing.T) {
	t.Run("CronIntervalValidator", func(t *testing.T) {
		t.Run("should fail for invalid and pass for valid notations", func(t *testing.T) {
			cases := []struct {
				TestData string
				IsValid  bool
			}{
				{
					TestData: "a b c d e",
					IsValid:  false,
				},
				{
					TestData: "bar bar",
					IsValid:  false,
				},
				{
					TestData: "@hello",
					IsValid:  false,
				},
				{
					TestData: "* * z",
					IsValid:  false,
				},
				{
					TestData: "@every 2h",
					IsValid:  true,
				},
				{
					TestData: "0 2 * * *",
					IsValid:  true,
				},
				{
					TestData: "0 2/3 * * *",
					IsValid:  true,
				},
				{
					TestData: "@midnight",
					IsValid:  true,
				},
				{
					TestData: "30 3-6,20-23 * * *",
					IsValid:  true,
				},
				{
					TestData: "@daily",
					IsValid:  true,
				},
			}

			for _, tcase := range cases {
				err := utils.CronIntervalValidator(tcase.TestData, "")
				if tcase.IsValid {
					assert.Nil(t, err)
				} else {
					assert.NotNil(t, err)
				}
			}
		})
	})
}
