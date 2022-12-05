package bigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestRelationalView(t *testing.T) {
	t.Run("return validation error when query is empty", func(t *testing.T) {
		view := bigquery.View{
			Name:      "t-optimus.playground.customer",
			ViewQuery: "",
		}

		err := view.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "view query is empty for t-optimus.playground.customer")
	})
	t.Run("has no validation error for correct view", func(t *testing.T) {
		view := bigquery.View{
			Name:      "t-optimus.playground.customer",
			ViewQuery: "select * from `t-optimus.playground.customer_table`",
		}

		err := view.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground.customer", view.Name.String())
	})
}
