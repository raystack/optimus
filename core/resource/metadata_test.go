package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestKind(t *testing.T) {
	t.Run("returns error on invalid type", func(t *testing.T) {
		_, err := resource.FromStringToKind("invalid")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: unknown kind invalid")
	})
	t.Run("returns correct kind", func(t *testing.T) {
		types := []string{
			"table", "view", "external_table", "dataset",
		}
		for _, typ := range types {
			kind, err := resource.FromStringToKind(typ)
			assert.Nil(t, err)
			assert.Equal(t, typ, kind.String())
		}
	})
}
