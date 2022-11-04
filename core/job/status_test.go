package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job"
)

func TestStatus(t *testing.T) {
	t.Run("StatusFrom", func(t *testing.T) {
		t.Run("returns the same status as the input string", func(t *testing.T) {
			testCases := []struct {
				input          string
				expectedOutput job.Status
			}{
				{
					input:          "unknown",
					expectedOutput: job.StatusUnknown,
				},
				{
					input:          "skipped",
					expectedOutput: job.StatusSkipped,
				},
				{
					input:          "validation_failure",
					expectedOutput: job.StatusValidationFailure,
				},
				{
					input:          "validation_success",
					expectedOutput: job.StatusValidationSuccess,
				},
				{
					input:          "to_create",
					expectedOutput: job.StatusToCreate,
				},
				{
					input:          "to_update",
					expectedOutput: job.StatusToUpdate,
				},
				{
					input:          "create_failure",
					expectedOutput: job.StatusCreateFailure,
				},
				{
					input:          "update_failure",
					expectedOutput: job.StatusUpdateFailure,
				},
				{
					input:          "success",
					expectedOutput: job.StatusSuccess,
				},
			}

			for _, tc := range testCases {
				input := tc.input

				expectedOutput := tc.expectedOutput

				actualOutput := job.StatusFrom(input)
				assert.Equal(t, expectedOutput, actualOutput)
			}
		})
		t.Run("returns unknown status if the input is not within the recognized values", func(t *testing.T) {
			unrecognizedStatus := "unrecognized"

			expectedOutput := job.StatusUnknown

			actualOutput := job.StatusFrom(unrecognizedStatus)
			assert.Equal(t, expectedOutput, actualOutput)
		})
	})
}
