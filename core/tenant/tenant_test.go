package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raystack/optimus/core/tenant"
)

func TestAggregateRootTenant(t *testing.T) {
	t.Run("Tenant", func(t *testing.T) {
		t.Run("return error when project name is empty", func(t *testing.T) {
			_, err := tenant.NewTenant("", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity project: project name is empty")
		})
		t.Run("returns error when namespace name is missing", func(t *testing.T) {
			_, err := tenant.NewTenant("t-optimus", "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity namespace: namespace name is empty")
		})
		t.Run("creates tenant with both project name and namespace name", func(t *testing.T) {
			tnnt, err := tenant.NewTenant("t-optimus", "n-optimus")
			assert.Nil(t, err)
			assert.Equal(t, "t-optimus", tnnt.ProjectName().String())

			namespaceName := tnnt.NamespaceName()
			assert.Equal(t, "n-optimus", namespaceName.String())
		})
	})
	t.Run("WithDetails", func(t *testing.T) {
		projectConf := map[string]string{
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
			"BUCKET":                     "gs://some_folder",
		}

		project, _ := tenant.NewProject("test-project", projectConf)
		namespace, _ := tenant.NewNamespace("test-ns", project.Name(), map[string]string{
			"BUCKET":       "gs://ns_folder",
			"OTHER_CONFIG": "optimus",
		})

		t.Run("return error when project not present", func(t *testing.T) {
			_, err := tenant.NewTenantDetails(nil, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity tenant: project is nil")
		})
		t.Run("return error when namespace not present", func(t *testing.T) {
			_, err := tenant.NewTenantDetails(project, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity tenant: namespace is nil")
		})
		t.Run("when both project and namespace are present", func(t *testing.T) {
			t.Run("return withDetails with project and namespace", func(t *testing.T) {
				tnnt, err := tenant.NewTenantDetails(project, namespace, nil)
				assert.Nil(t, err)

				p := tnnt.Project()
				assert.NotNil(t, p)
				assert.Equal(t, "test-project", p.Name().String())

				ns := tnnt.Namespace()
				assert.NotNil(t, ns)
				assert.Equal(t, "test-ns", ns.Name().String())
			})
			t.Run("returns merged config", func(t *testing.T) {
				tnnt, err := tenant.NewTenantDetails(project, namespace, nil)
				assert.Nil(t, err)

				assert.Equal(t, 4, len(tnnt.GetConfigs()))
			})
			t.Run("returns an error when key not present", func(t *testing.T) {
				tnnt, err := tenant.NewTenantDetails(project, namespace, nil)
				assert.Nil(t, err)

				_, err = tnnt.GetConfig("NON-EXISTENT")
				assert.NotNil(t, err)
				assert.EqualError(t, err, "not found for entity tenant: config not present in tenant NON-EXISTENT")
			})
			t.Run("returns a config giving priority to namespace", func(t *testing.T) {
				tnnt, err := tenant.NewTenantDetails(project, namespace, nil)
				assert.Nil(t, err)

				val, err := tnnt.GetConfig("BUCKET")
				assert.Nil(t, err)
				assert.Equal(t, "gs://ns_folder", val)
			})
			t.Run("returns a config from project", func(t *testing.T) {
				details, err := tenant.NewTenantDetails(project, namespace, nil)
				assert.Nil(t, err)

				val, err := details.GetConfig(tenant.ProjectStoragePathKey)
				assert.Nil(t, err)
				assert.Equal(t, "gs://location", val)
			})
			t.Run("returns tenant", func(t *testing.T) {
				details, err := tenant.NewTenantDetails(project, namespace, nil)
				assert.Nil(t, err)

				tnnt := details.ToTenant()
				assert.Equal(t, "test-project", tnnt.ProjectName().String())

				ns := tnnt.NamespaceName()
				assert.Equal(t, "test-ns", ns.String())
			})
			t.Run("returns secrets", func(t *testing.T) {
				p1, _ := tenant.NewPlainTextSecret("key", "value")
				p2, _ := tenant.NewPlainTextSecret("key2", "value2")
				secrets := tenant.PlainTextSecrets{p1, p2}

				details, err := tenant.NewTenantDetails(project, namespace, secrets)
				assert.NoError(t, err)

				secMap := details.SecretsMap()
				assert.Len(t, secMap, 2)
				assert.Equal(t, "value2", secMap[p2.Name().String()])
			})
		})
	})
}
