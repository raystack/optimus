package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
)

func TestResource(t *testing.T) {
	tnnt, tnntErr := tenant.NewTenant("proj", "ns")
	assert.Nil(t, tnntErr)

	t.Run("returns error when name is empty", func(t *testing.T) {
		_, err := resource.NameFrom("")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: resource name is empty")
	})
	t.Run("returns name when name is proper", func(t *testing.T) {
		name, err := resource.NameFrom("resource_name")
		assert.Nil(t, err)
		assert.Equal(t, "resource_name", name.String())
	})
	t.Run("when invalid resource", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			_, err := resource.NewResource("", resource.KindTable, resource.BigQuery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid resource name: ")
		})
		t.Run("returns error when dataset name is empty", func(t *testing.T) {
			_, err := resource.NewResource("", resource.KindDataset, resource.BigQuery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid dataset name: ")
		})
		t.Run("returns error when invalid resource name", func(t *testing.T) {
			_, err := resource.NewResource("proj.set.", resource.KindTable, resource.BigQuery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: resource name is empty")
		})
		t.Run("returns error when invalid dataset name", func(t *testing.T) {
			_, err := resource.NewResource("proj.", resource.KindDataset, resource.BigQuery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: resource name is empty")
		})
		t.Run("returns error when resource has invalid dataset name", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			_, err := resource.NewResource("proj..name1", resource.KindTable, resource.BigQuery, tnnt, nil, spec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: schema/dataset name is empty")
		})
		t.Run("returns error when invalid resource metadata", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			_, err := resource.NewResource("proj.set.res_name", resource.KindTable, resource.BigQuery, tnnt, nil, spec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid resource metadata")
		})
		t.Run("returns error when invalid resource metadata", func(t *testing.T) {
			meta := resource.Metadata{
				Version:     1,
				Description: "description",
			}
			_, err := resource.NewResource("proj.set.res_name", resource.KindTable,
				resource.BigQuery, tnnt, &meta, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid resource spec "+
				"for proj.set.res_name")
		})
	})
	t.Run("creates table resource successfully", func(t *testing.T) {
		meta := resource.Metadata{
			Version:     1,
			Description: "description",
		}
		spec := map[string]any{"a": "b"}
		res, err := resource.NewResource("proj.set.res_name", resource.KindTable,
			resource.BigQuery, tnnt, &meta, spec)
		assert.Nil(t, err)

		assert.Equal(t, "proj.set.res_name", res.FullName())
		assert.Equal(t, "bigquery://proj:set.res_name", res.URN())
		assert.Equal(t, "res_name", res.Name().String())
		assert.Equal(t, "table", res.Kind().String())
		assert.Equal(t, "proj", res.Tenant().ProjectName().String())
		assert.Equal(t, "description", res.Metadata().Description)
		assert.Equal(t, "b", res.Spec()["a"])
	})
	t.Run("creates dataset object successfully", func(t *testing.T) {
		meta := resource.Metadata{
			Version:     1,
			Description: "description",
		}
		spec := map[string]any{"a": "b"}
		res, err := resource.NewResource("proj.dataset", resource.KindDataset,
			resource.BigQuery, tnnt, &meta, spec)
		assert.Nil(t, err)

		assert.Equal(t, "proj.dataset", res.FullName())
		assert.Equal(t, "bigquery://proj:dataset", res.URN())
		assert.Equal(t, "dataset", res.Name().String())
		assert.Equal(t, "dataset", res.Kind().String())
	})
	t.Run("Validate", func(t *testing.T) {
		invalidSpec := map[string]any{"description": "some desc"}
		t.Run("returns error for unknown kind", func(t *testing.T) {
			res := resource.Resource{}
			err := res.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: unknown kind")
		})
		t.Run("returns error when view spec is not proper", func(t *testing.T) {
			res, err := resource.NewResource("proj.set.view_name1", resource.KindView, resource.BigQuery,
				tnnt, &resource.Metadata{}, invalidSpec)
			assert.Nil(t, err)

			assert.Equal(t, "proj.set.view_name1", res.FullName())

			err = res.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_view: view query is empty "+
				"for proj.set.view_name1")
		})
		t.Run("returns error when external_table spec is invalid", func(t *testing.T) {
			res, err := resource.NewResource("proj.set.external_name1", resource.KindExternalTable, resource.BigQuery,
				tnnt, &resource.Metadata{}, invalidSpec)
			assert.Nil(t, err)

			assert.Equal(t, "proj.set.external_name1", res.FullName())

			err = res.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: invalid schema "+
				"for proj.set.external_name1")
		})
		t.Run("returns error when cannot decode table", func(t *testing.T) {
			res, err := resource.NewResource("proj.set.table_name1", resource.KindTable, resource.BigQuery,
				tnnt, &resource.Metadata{}, invalidSpec)
			assert.Nil(t, err)

			assert.Equal(t, "proj.set.table_name1", res.FullName())

			err = res.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: empty "+
				"schema for table proj.set.table_name1")
		})
		t.Run("returns no error when can decode dataset", func(t *testing.T) {
			res, err := resource.NewResource("proj.set_name1", resource.KindDataset, resource.BigQuery,
				tnnt, &resource.Metadata{}, invalidSpec)
			assert.Nil(t, err)

			err = res.Validate()
			assert.Nil(t, err)
		})
	})
}
