package instance_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/instance"
	"testing"
)

func TestMacroCompiler(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		t.Run("should return compiled string with values of macros", func(t *testing.T) {
			testCases := []struct {
				Input    string
				Expected string
			}{
				{
					"event_timestamp > \"{{.DSTART}}\" AND event_timestamp <= \"{{.DEND}}\"",
					"event_timestamp > \"2021-02-10 10:00:00\" AND event_timestamp <= \"2021-02-11 10:00:00\"",
				},
				{
					"event_timestamp > {{.DSTART}} AND event_timestamp <= {{.DEND}}",
					"event_timestamp > 2021-02-10 10:00:00 AND event_timestamp <= 2021-02-11 10:00:00",
				},
				{
					"event_timestamp > \"{{.EXECUTION_TIME}}\"",
					"event_timestamp > \"empty val\"",
				},
				{
					"event_timestamp > .DSTART AND event_timestamp <= .DEND",
					"event_timestamp > .DSTART AND event_timestamp <= .DEND",
				},
			}

			for _, testCase := range testCases {
				values := map[string]string{
					"DSTART":         "2021-02-10 10:00:00",
					"DEND":           "2021-02-11 10:00:00",
					"EXECUTION_TIME": "empty val",
				}

				comp := instance.NewMacroCompiler()
				compiledExpr, err := comp.CompileTemplate(values, testCase.Input)

				assert.Nil(t, err)
				assert.Equal(t, testCase.Expected, compiledExpr)
			}
		})
	})
}
