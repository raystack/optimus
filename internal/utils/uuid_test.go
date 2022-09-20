package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/utils"
)

func TestUUID(t *testing.T) {
	t.Run("NewUUID", func(t *testing.T) {
		t.Run("should generate random UUID", func(t *testing.T) {
			uuidProvider := utils.NewUUIDProvider()
			uuid, err := uuidProvider.NewUUID()
			assert.NotNil(t, uuid)
			assert.Nil(t, err)
		})
	})
}
