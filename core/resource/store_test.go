package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestDataStore(t *testing.T) {
	t.Run("returns error when unknown type", func(t *testing.T) {
		_, err := resource.FromStringToStore("invalid")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: unknown store invalid")
	})
	t.Run("converts a string to store when correct", func(t *testing.T) {
		bq, err := resource.FromStringToStore("bigquery")
		assert.Nil(t, err)
		assert.Equal(t, "bigquery", bq.String())
	})
}
