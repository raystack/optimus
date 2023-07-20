package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raystack/optimus/core/tenant"
)

func TestEntityNamespace(t *testing.T) {
	t.Run("NamespaceName", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			_, err := tenant.NamespaceNameFrom("")

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity namespace: namespace name is empty")
		})
		t.Run("creates a name when proper", func(t *testing.T) {
			name, err := tenant.NamespaceNameFrom("t-namespace")

			assert.Nil(t, err)
			assert.Equal(t, "t-namespace", name.String())
		})
	})
	t.Run("Namespace", func(t *testing.T) {
		projName, _ := tenant.ProjectNameFrom("optimus-proj")

		t.Run("return error when name is empty", func(t *testing.T) {
			_, err := tenant.NewNamespace("", projName, map[string]string{})

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity namespace: namespace name is empty")
		})
		t.Run("return error when validation fails due to project name", func(t *testing.T) {
			_, err := tenant.NewNamespace("t-namespace", "", map[string]string{})

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity namespace: project name is empty")
		})
		t.Run("creates namespace object", func(t *testing.T) {
			ns, err := tenant.NewNamespace("t-namespace", projName, map[string]string{"a": "b"})

			assert.Nil(t, err)
			assert.Equal(t, "t-namespace", ns.Name().String())
			assert.Equal(t, projName.String(), ns.ProjectName().String())

			assert.NotNil(t, ns.GetConfigs())
			config, err := ns.GetConfig("a")
			assert.Nil(t, err)
			assert.Equal(t, "b", config)

			_, err = ns.GetConfig("non-existent")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity namespace: namespace config not found non-existent")
		})
	})
}
