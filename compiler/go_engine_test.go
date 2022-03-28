package compiler_test

import (
	"testing"

	"github.com/odpf/optimus/compiler"
	"github.com/stretchr/testify/assert"
)

func TestGoEngine(t *testing.T) {
	t.Run("CompileString", func(t *testing.T) {
		t.Run("should return compiled string with values of macros", func(t *testing.T) {
			testCases := []struct {
				Input    string
				Expected string
			}{
				{
					"event_timestamp > \"{{.DSTART}}\" AND event_timestamp <= \"{{.DEND}}\"",
					"event_timestamp > \"2021-02-10T10:00:00+00:00\" AND event_timestamp <= \"2021-02-11T10:00:00+00:00\"",
				},
				{
					"event_timestamp > {{.DSTART}} AND event_timestamp <= {{.DEND}}",
					"event_timestamp > 2021-02-10T10:00:00+00:00 AND event_timestamp <= 2021-02-11T10:00:00+00:00",
				},
				{
					"event_timestamp > \"{{.EXECUTION_TIME}}\"",
					"event_timestamp > \"empty val\"",
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
			}

			for _, testCase := range testCases {
				values := map[string]interface{}{
					"DSTART":         "2021-02-10T10:00:00+00:00",
					"DEND":           "2021-02-11T10:00:00+00:00",
					"EXECUTION_TIME": "empty val",
				}

				comp := compiler.NewGoEngine()
				compiledExpr, err := comp.CompileString(testCase.Input, values)

				assert.Nil(t, err)
				assert.Equal(t, testCase.Expected, compiledExpr)
			}
		})
	})
	t.Run("CompileFiles", func(t *testing.T) {
		t.Run("should return rendered string with values of macros/partials for files", func(t *testing.T) {
			testCases := []struct {
				Input    map[string]string
				Expected map[string]string
			}{
				{
					map[string]string{
						"query": `
				event_timestamp > \"{{.DSTART}}\" AND event_timestamp <= \"{{.DEND}}\"
				`,
					},
					map[string]string{
						"query": `
				event_timestamp > \"2021-02-10T10:00:00+00:00\" AND event_timestamp <= \"2021-02-11T10:00:00+00:00\"
				`,
					},
				},
				{
					map[string]string{
						"query":         `{{ template "partials.tmpl" "d"}}using var`,
						"partials.tmpl": `declare today date = {{ . }};`,
					},
					map[string]string{
						"query":         `declare today date = d;using var`,
						"partials.tmpl": `declare today date = {{ . }};`,
					},
				},
				{
					map[string]string{
						"query": `{{$ctx := dict "somevar" "hello" "root" .}}
{{- template "partials.tmpl" $ctx}}using var`,
						"partials.tmpl": `declare today date = {{ .somevar }};{{.root.EXECUTION_TIME}};`,
					},
					map[string]string{
						"query":         `declare today date = hello;empty val;using var`,
						"partials.tmpl": `declare today date = {{ .somevar }};{{.root.EXECUTION_TIME}};`,
					},
				},
				{
					map[string]string{
						"query": `Name: {{ template "name"}}, Gender: {{ template "gender" }}`,
						"partials.tmpl": `
{{- define "name" -}} Adam {{- end}}
{{- define "gender" -}} Male {{- end}}
`,
					},
					map[string]string{
						"query": `Name: Adam, Gender: Male`,
						"partials.tmpl": `
{{- define "name" -}} Adam {{- end}}
{{- define "gender" -}} Male {{- end}}
`,
					},
				},
			}

			for _, testCase := range testCases {
				values := map[string]interface{}{
					"DSTART":         "2021-02-10T10:00:00+00:00",
					"DEND":           "2021-02-11T10:00:00+00:00",
					"EXECUTION_TIME": "empty val",
				}

				comp := compiler.NewGoEngine()
				compiledExpr, err := comp.CompileFiles(testCase.Input, values)
				if err != nil {
					t.Error(err)
				}
				assert.Nil(t, err)
				assert.Equal(t, testCase.Expected, compiledExpr)
			}
		})
	})
}
