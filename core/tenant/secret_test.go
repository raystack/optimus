package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/tenant"
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

			assert.Equal(t, "secret_name", pts.Name())
			assert.Equal(t, "secret_val", pts.Value())
		})
	})
	t.Run("Secret", func(t *testing.T) {
		t.Run("SecretType", func(t *testing.T) {
			t.Run("returns error when unknown type", func(t *testing.T) {
				unknown := "unknown"
				_, err := tenant.SecretTypeFromString(unknown)
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity secret: unknown type for secret type: unknown")
			})
			t.Run("returns user defined type for valid string", func(t *testing.T) {
				typ, err := tenant.SecretTypeFromString(tenant.UserDefinedSecret.String())
				assert.Nil(t, err)
				assert.Equal(t, tenant.UserDefinedSecret.String(), typ.String())
			})
			t.Run("returns system defined type for valid string", func(t *testing.T) {
				typ, err := tenant.SecretTypeFromString(tenant.SystemDefinedSecret.String())
				assert.Nil(t, err)
				assert.Equal(t, tenant.SystemDefinedSecret.String(), typ.String())
			})
		})
		t.Run("returns error when name is empty", func(t *testing.T) {
			_, err := tenant.NewSecret("", tenant.UserDefinedSecret, "", tenant.Tenant{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: secret name is empty")
		})
		t.Run("returns error when type is invalid", func(t *testing.T) {
			_, err := tenant.NewSecret("name", "unknown", "", tenant.Tenant{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: invalid secret type")
		})
		t.Run("returns error when encodedValue is empty", func(t *testing.T) {
			_, err := tenant.NewSecret("name", tenant.UserDefinedSecret, "", tenant.Tenant{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: empty encoded secret")
		})
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			_, err := tenant.NewSecret("name", tenant.UserDefinedSecret, "encoded==", tenant.Tenant{})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity secret: invalid tenant details")
		})
		t.Run("returns secret", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant("test-project", "test-ns")

			s, err := tenant.NewSecret("name", tenant.UserDefinedSecret, "encoded==", tnnt)

			assert.Nil(t, err)
			assert.Equal(t, "name", s.Name())
			assert.Equal(t, "user", s.Type().String())
			assert.Equal(t, "encoded==", s.EncodedValue())
			assert.Equal(t, tnnt, s.Tenant())
		})
	})
}
