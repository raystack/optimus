package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestRelationalView(t *testing.T) {
	t.Run("return validation error when query is empty", func(t *testing.T) {
		ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		view := resource.View{
			Name:      "customer",
			Dataset:   ds,
			ViewQuery: "",
		}

		err = view.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_view: view query is empty "+
			"for t-optimus.playground.customer")
	})
	t.Run("has no validation error for correct view", func(t *testing.T) {
		ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		view := resource.View{
			Name:      "customer",
			Dataset:   ds,
			ViewQuery: "select * from `t-optimus.playground.customer_table`",
		}

		err = view.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground.customer", view.FullName())
		assert.Equal(t, "bigquery://t-optimus:playground.customer", view.URN())
	})
}
