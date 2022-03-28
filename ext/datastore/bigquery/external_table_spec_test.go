package bigquery //nolint: testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExternalTableSpecHandler(t *testing.T) {
	t.Run("should generate urn successfully", func(t *testing.T) {
		project := "sample-project"
		dataset := "sample-dataset"
		table := "sample-table"

		urn, err := externalTableSpec{}.GenerateURN(BQTable{
			Project: project,
			Dataset: dataset,
			Table:   table,
		})

		assert.Nil(t, err)
		assert.Equal(t, "bigquery://sample-project:sample-dataset.sample-table", urn)
	})
}
