package compiler_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/compiler"
)

func TestEngine(t *testing.T) {
	t.Run("CompileString", func(t *testing.T) {
		t.Run("returns error when cannot parse macro", func(t *testing.T) {
			input := `event_timestamp > "{{.DSTART"`
			context := map[string]interface{}{
				"DSTART": "2021-02-10T10:00:00+00:00",
			}

			comp := compiler.NewEngine()
			_, err := comp.CompileString(input, context)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity compiler: unable to parse string event_timestamp > \"{{.DSTART\"")
		})
		t.Run("returns error when rendering fails", func(t *testing.T) {
			input := `event_timestamp > "{{.DSTART | Date }}"`
			context := map[string]interface{}{
				"DSTART": "",
			}

			comp := compiler.NewEngine()
			_, err := comp.CompileString(input, context)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity compiler: unable to render string event_timestamp > \"{{.DSTART | Date }}\"")
		})
		t.Run("returns compiled string with values of macros", func(t *testing.T) {
			testCases := []struct {
				Input    string
				Expected string
			}{
				{
					`event_timestamp > "{{.DSTART}}" AND event_timestamp <= "{{.DEND}}"`,
					`event_timestamp > "2021-02-10T10:00:00+00:00" AND event_timestamp <= "2021-02-11T10:00:00+00:00"`,
				},
				{
					"event_timestamp > {{.DSTART}} AND event_timestamp <= {{.DEND}}",
					"event_timestamp > 2021-02-10T10:00:00+00:00 AND event_timestamp <= 2021-02-11T10:00:00+00:00",
				},
				{
					`event_timestamp > "{{.EXECUTION_TIME}}"`,
					`event_timestamp > "empty val"`,
				},
				{
					"event_timestamp > .DSTART AND event_timestamp <= .DEND",
					"event_timestamp > .DSTART AND event_timestamp <= .DEND",
				},
				{
					"event_timestamp > {{ .DSTART|Date }} AND event_timestamp <= {{.DEND}}",
					"event_timestamp > 2021-02-10 AND event_timestamp <= 2021-02-11T10:00:00+00:00",
				},
				{
					"event_timestamp > {{ .DSTART | Date }} AND event_timestamp <= {{ Date .DEND }}",
					"event_timestamp > 2021-02-10 AND event_timestamp <= 2021-02-11",
				},
				{
					`{{ .DSTART | Date | toDate "2006-01-02" | date "Jan 2, 2006" }}`,
					"Feb 10, 2021",
				},
				{
					`{{ .DSTART | Date | toDate "2006-01-02" | date_modify "-24h" | date "2006-01-02" }}`,
					"2021-02-09",
				},
			}

			for _, testCase := range testCases {
				values := map[string]interface{}{
					"DSTART":         "2021-02-10T10:00:00+00:00",
					"DEND":           "2021-02-11T10:00:00+00:00",
					"EXECUTION_TIME": "empty val",
				}

				comp := compiler.NewEngine()
				compiledExpr, err := comp.CompileString(testCase.Input, values)

				assert.Nil(t, err)
				assert.Equal(t, testCase.Expected, compiledExpr)
			}
		})
	})
	t.Run("Compile", func(t *testing.T) {
		t.Run("returns error when cannot parse macro", func(t *testing.T) {
			input := `event_timestamp > "{{.DSTART"`
			context := map[string]interface{}{
				"DSTART": "2021-02-10T10:00:00+00:00",
			}

			comp := compiler.NewEngine()
			_, err := comp.Compile(map[string]string{"query": input}, context)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity compiler: unable to parse content for query")
		})
		t.Run("returns error when rendering fails", func(t *testing.T) {
			input := `event_timestamp > "{{.DSTART | Date }}"`
			context := map[string]interface{}{
				"DSTART": "",
			}

			comp := compiler.NewEngine()
			_, err := comp.Compile(map[string]string{"query": input}, context)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity compiler: unable to render content for query")
		})
		t.Run("returns rendered string with values of macros for template map", func(t *testing.T) {
			testCases := []struct {
				Input    map[string]string
				Expected map[string]string
			}{
				{
					map[string]string{
						"query": `
				event_timestamp > "{{.DSTART}}" AND event_timestamp <= "{{.DEND}}"
				`,
					},
					map[string]string{
						"query": `event_timestamp > "2021-02-10T10:00:00+00:00" AND event_timestamp <= "2021-02-11T10:00:00+00:00"`,
					},
				}, {
					map[string]string{
						"query": `
				event_timestamp > "{{.DSTART | Date }}" AND event_timestamp <= "{{.DEND | Date }}"
				`,
					},
					map[string]string{
						"query": `event_timestamp > "2021-02-10" AND event_timestamp <= "2021-02-11"`,
					},
				},
			}

			for _, testCase := range testCases {
				context := map[string]interface{}{
					"DSTART":         "2021-02-10T10:00:00+00:00",
					"DEND":           "2021-02-11T10:00:00+00:00",
					"EXECUTION_TIME": "empty val",
				}

				comp := compiler.NewEngine()
				compiledExpr, err := comp.Compile(testCase.Input, context)
				if err != nil {
					t.Error(err)
				}
				assert.Nil(t, err)
				assert.Equal(t, testCase.Expected, compiledExpr)
			}
		})
	})
}
