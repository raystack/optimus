package utils_test

import (
	"fmt"
	"testing"

	"github.com/odpf/optimus/utils"
	"github.com/stretchr/testify/assert"
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
				{
					TestData: "",
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

	t.Run("NewFromRegex", func(t *testing.T) {
		t.Run("should return message if regex fails to match", func(t *testing.T) {
			cases := []struct {
				Regex, TestData, Message string
				IsValid                  bool
			}{
				{
					Regex:    `foo`,
					TestData: "foo",
					Message:  "invalid",
					IsValid:  true,
				},
				{
					Regex:    `foo`,
					TestData: "bar",
					Message:  "invalid",
					IsValid:  false,
				},
			}

			factory := new(utils.VFactory)
			for _, tcase := range cases {
				validator := factory.NewFromRegex(tcase.Regex, tcase.Message)
				err := validator(tcase.TestData)
				if tcase.IsValid {
					assert.Nil(t, err)
				} else {
					assert.Equal(t, fmt.Errorf(tcase.Message), err)
				}
			}
		})
		t.Run("should panic if the regex provided is invalid", func(t *testing.T) {
			defer func() {
				if err := recover(); err == nil {
					t.Error("expected validator to throw an exception, but it didnt")
				}
			}()

			new(utils.VFactory).NewFromRegex(`[`, "boom")
		})
	})
	t.Run("ValidateCronInterval", func(t *testing.T) {
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
				err := utils.ValidateCronInterval(tcase.TestData)
				if tcase.IsValid {
					assert.Nil(t, err)
				} else {
					assert.NotNil(t, err)
				}
			}
		})
	})
}
