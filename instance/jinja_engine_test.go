package instance_test

import (
	"testing"
	"time"

	"github.com/odpf/optimus/models"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/instance"
)

func TestJinjaCompiler(t *testing.T) {
	t.Run("CompileString", func(t *testing.T) {
		t.Run("should return compiled string with values of macros", func(t *testing.T) {
			testCases := []struct {
				Input    string
				Expected string
			}{
				{
					"event_timestamp > \"{{DSTART}}\" AND event_timestamp <= \"{{DEND}}\"",
					"event_timestamp > \"2021-02-10T10:00:00+00:00\" AND event_timestamp <= \"2021-02-11T10:00:00+00:00\"",
				},
				{
					"event_timestamp > {{DSTART}} AND event_timestamp <= {{DEND}}",
					"event_timestamp > 2021-02-10T10:00:00+00:00 AND event_timestamp <= 2021-02-11T10:00:00+00:00",
				},
				{
					"event_timestamp > \"{{EXECUTION_TIME}}\"",
					"event_timestamp > \"empty val\"",
				},
				{
					"event_timestamp > .DSTART AND event_timestamp <= .DEND",
					"event_timestamp > .DSTART AND event_timestamp <= .DEND",
				},
				{
					"event_timestamp > {{ Date(DSTART) }} AND event_timestamp <= {{DEND}}",
					"event_timestamp > 2021-02-10 AND event_timestamp <= 2021-02-11T10:00:00+00:00",
				},
				{
					"event_timestamp > {{ DSTART|ToDate }} AND event_timestamp <= {{DEND}}",
					"event_timestamp > 2021-02-10 AND event_timestamp <= 2021-02-11T10:00:00+00:00",
				},
				{
					`{% set vv = 2 %} {{vv}}`,
					" 2",
				},
				{
					`{% list vv = 2 3 4 6 %}
{%- for v in vv -%}{{ v }},{%- endfor -%}`,
					"2,3,4,6,",
				},
			}

			for _, testCase := range testCases {
				values := map[string]interface{}{
					"DSTART":         "2021-02-10T10:00:00+00:00",
					"DEND":           "2021-02-11T10:00:00+00:00",
					"EXECUTION_TIME": "empty val",
					"Date": func(timeStr string) (string, error) {
						t, err := time.Parse(models.InstanceScheduledAtTimeLayout, timeStr)
						if err != nil {
							return "", err
						}
						return t.Format(models.JobDatetimeLayout), nil
					},
				}

				comp := instance.NewJinjaEngine()
				compiledExpr, err := comp.CompileString(testCase.Input, values)

				if err != nil {
					t.Error(err)
				}
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
				event_timestamp > \"{{DSTART}}\" AND event_timestamp <= \"{{DEND}}\"
				`,
						"test.tmpl": "{{ DSTART }}",
					},
					map[string]string{
						"query": `
				event_timestamp > \"2021-02-10T10:00:00+00:00\" AND event_timestamp <= \"2021-02-11T10:00:00+00:00\"
				`,
						"test.tmpl": "{{ DSTART }}",
					},
				},
				{
					map[string]string{
						"query": `
				{% import "macros.tplz" greetings -%}
				event_timestamp > \"{{DSTART}}\" AND event_timestamp <= \"{{DEND}}\"
				{{- greetings("rick") -}}`,
						"macros.tplz": `
				{%- macro greetings(to, at=DSTART, name2="guest") export %}
				Greetings to {{ to }} at {{ at }}. Howdy, {% if name2 == "guest" %}anonymous guest{% else %}{{ name2 }}{% endif %}!
				{%- endmacro %}`,
					},
					map[string]string{
						"query": `
				event_timestamp > \"2021-02-10T10:00:00+00:00\" AND event_timestamp <= \"2021-02-11T10:00:00+00:00\"
				Greetings to rick at 2021-02-10T10:00:00+00:00. Howdy, anonymous guest!`,
						"macros.tplz": ``,
					},
				},
				{
					map[string]string{
						"query": `
{%- include "partials.tpl" %}
event_timestamp > \"{{DSTART}}\" AND event_timestamp <= \"{{DEND}}\"
`,
						"partials.tpl": `Hello world`,
					},
					map[string]string{
						"query": `Hello world
event_timestamp > \"2021-02-10T10:00:00+00:00\" AND event_timestamp <= \"2021-02-11T10:00:00+00:00\"
`,
						"partials.tpl": `Hello world`,
					},
				},
			}

			for _, testCase := range testCases {
				values := map[string]interface{}{
					"DSTART":         "2021-02-10T10:00:00+00:00",
					"DEND":           "2021-02-11T10:00:00+00:00",
					"EXECUTION_TIME": "empty val",
					"Date": func(timeStr string) (string, error) {
						t, err := time.Parse(models.InstanceScheduledAtTimeLayout, timeStr)
						if err != nil {
							return "", err
						}
						return t.Format(models.JobDatetimeLayout), nil
					},
				}

				comp := instance.NewJinjaEngine()
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
