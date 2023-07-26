package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/tenant"
)

func TestEntitySecret(t *testing.T) {
	t.Run("PlainTextSecret", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			_, err := tenant.NewPlainTextSecret("", "val")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when value is empty", func(t *testing.T) {
			_, err := tenant.NewPlainTextSecret("name", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: empty secret value")
		})
		t.Run("creates the object", func(t *testing.T) {
			pts, err := tenant.NewPlainTextSecret("secret_name", "secret_val")
			assert.Nil(t, err)

			assert.Equal(t, "SECRET_NAME", pts.Name().String())
			assert.Equal(t, "secret_val", pts.Value())
		})
	})
	t.Run("Secret", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			_, err := tenant.NewSecret("", "", "", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when encodedValue is empty", func(t *testing.T) {
			_, err := tenant.NewSecret("name", "", "", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: empty encoded secret")
		})
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			_, err := tenant.NewSecret("name", "encoded==", "", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: invalid tenant details")
		})
		t.Run("returns secret", func(t *testing.T) {
			projName, _ := tenant.ProjectNameFrom("test-project")
			nsName := "test-ns"

			s, err := tenant.NewSecret("name", "encoded==", projName, nsName)

			assert.Nil(t, err)
			assert.Equal(t, "NAME", s.Name().String())
			assert.Equal(t, "encoded==", s.EncodedValue())
			assert.Equal(t, projName.String(), s.ProjectName().String())
			assert.Equal(t, nsName, s.NamespaceName())
		})
	})
}
