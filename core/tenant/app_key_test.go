package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/tenant"
)

func TestApplicationKey(t *testing.T) {
	t.Run("ApplicationKey", func(t *testing.T) {
		t.Run("return error when invalid key", func(t *testing.T) {
			appKeyStr := "28charshtesthashtesthashtest"
			_, err := tenant.NewApplicationKey(appKeyStr)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity application_key: random hash should be 32 chars in length")
		})
		t.Run("return app key when valid", func(t *testing.T) {
			appKeyStr := "32charshtesthashtesthashtesthash"
			k, err := tenant.NewApplicationKey(appKeyStr)

			assert.Nil(t, err)
			keyContent := k.GetKey()
			assert.Equal(t, appKeyStr, string(keyContent[:]))
		})
	})
}
