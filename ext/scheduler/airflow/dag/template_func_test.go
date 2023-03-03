package dag_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/scheduler/airflow/dag"
)

func TestTemplateFuncMap(t *testing.T) {
	t.Run("Replace", func(t *testing.T) {
		str := "string_with_underscore"
		newStr := dag.Replace("_", "-", str)

		assert.Equal(t, "string-with-underscore", newStr)
	})
	t.Run("Quote", func(t *testing.T) {
		str := "string value"
		quotedString := dag.Quote(str)

		assert.Equal(t, fmt.Sprintf("%q", str), quotedString)
	})
	t.Run("Trunc", func(t *testing.T) {
		str := "this is a string which is 44 characters long, this extra portion is 37 chars long"

		truncated := dag.Trunc(44, str)
		assert.Equal(t, "this is a string which is 44 characters long", truncated)
	})
	t.Run("ReplaceDash", func(t *testing.T) {
		str := "string-with-dash-should-be-replaced"

		replaced := dag.ReplaceDash(str)
		assert.Equal(t, "string__dash__with__dash__dash__dash__should__dash__be__dash__replaced", replaced)
	})
	t.Run("DisplayName", func(t *testing.T) {
		str := "bq2bq-transform.bigquery"

		displayName := dag.DisplayName(str)
		assert.Equal(t, "bq2bq__dash__transform__dot__bigquery", displayName)
	})
}
