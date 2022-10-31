package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
)

func TestName(t *testing.T) {
	t.Run("NameFrom", func(t *testing.T) {
		t.Run("returns empty and error if name is empty", func(t *testing.T) {
			name, err := resource.NameFrom("")
			assert.Empty(t, name.String())
			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: resource name is empty")
		})

		t.Run("returns name and nil if name is proper", func(t *testing.T) {
			name, err := resource.NameFrom("resource_name")
			assert.Equal(t, "resource_name", name.String())
			assert.NoError(t, err)
		})
	})
}

func TestNewResource(t *testing.T) {
	tnnt, tnntErr := tenant.NewTenant("proj", "ns")
	assert.Nil(t, tnntErr)

	t.Run("when invalid resource", func(t *testing.T) {
		t.Run("returns error when name is empty", func(t *testing.T) {
			_, err := resource.NewResource("", resource.KindTable, resource.Bigquery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid resource name: ")
		})
		t.Run("returns error when dataset name is empty", func(t *testing.T) {
			_, err := resource.NewResource("", resource.KindDataset, resource.Bigquery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid dataset name: ")
		})
		t.Run("returns error when invalid resource name", func(t *testing.T) {
			_, err := resource.NewResource("proj.set.", resource.KindTable, resource.Bigquery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: resource name is empty")
		})
		t.Run("returns error when invalid dataset name", func(t *testing.T) {
			_, err := resource.NewResource("proj.", resource.KindDataset, resource.Bigquery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: resource name is empty")
		})
		t.Run("returns error when resource has invalid dataset name", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			_, err := resource.NewResource("proj..name1", resource.KindTable, resource.Bigquery, tnnt, nil, spec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_dataset: schema/dataset name is empty")
		})
		t.Run("returns error when invalid resource metadata", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			_, err := resource.NewResource("proj.set.res_name", resource.KindTable, resource.Bigquery, tnnt, nil, spec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid resource metadata")
		})
		t.Run("returns error when invalid resource metadata", func(t *testing.T) {
			meta := resource.Metadata{
				Version:     1,
				Description: "description",
			}
			_, err := resource.NewResource("proj.set.res_name", resource.KindTable,
				resource.Bigquery, tnnt, &meta, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: invalid resource spec "+
				"for proj.set.res_name")
		})
	})
	t.Run("creates table resource successfully", func(t *testing.T) {
		meta := &resource.Metadata{
			Version:     1,
			Description: "description",
		}
		spec := map[string]any{"a": "b"}
		res, err := resource.NewResource("proj.set.res_name", resource.KindTable,
			resource.Bigquery, tnnt, meta, spec)
		assert.Nil(t, err)

		assert.Equal(t, "res_name", res.Name().String())
		assert.Equal(t, "proj.set.res_name", res.FullName())
		assert.Equal(t, "bigquery://proj:set.res_name", res.URN())
		assert.EqualValues(t, meta, res.Metadata())
		assert.Equal(t, "table", res.Kind().String())
		assert.EqualValues(t, tnnt, res.Tenant())
		dataset, err := resource.DataSetFrom(resource.Bigquery, "proj", "set")
		assert.NoError(t, err)
		assert.EqualValues(t, dataset, res.Dataset())
		assert.Equal(t, resource.StatusUnknown, res.Status())
		assert.EqualValues(t, spec, res.Spec())
	})
	t.Run("creates dataset object successfully", func(t *testing.T) {
		meta := &resource.Metadata{
			Version:     1,
			Description: "description",
		}
		spec := map[string]any{"a": "b"}
		res, err := resource.NewResource("proj.dataset", resource.KindDataset,
			resource.Bigquery, tnnt, meta, spec)
		assert.Nil(t, err)

		assert.Equal(t, "dataset", res.Name().String())
		assert.Equal(t, "proj.dataset", res.FullName())
		assert.Equal(t, "bigquery://proj:dataset", res.URN())
		assert.EqualValues(t, meta, res.Metadata())
		assert.Equal(t, "dataset", res.Kind().String())
		assert.EqualValues(t, tnnt, res.Tenant())
		dataset, err := resource.DataSetFrom(resource.Bigquery, "proj", "dataset")
		assert.NoError(t, err)
		assert.EqualValues(t, dataset, res.Dataset())
		assert.Equal(t, resource.StatusUnknown, res.Status())
		assert.EqualValues(t, spec, res.Spec())
	})
}

func TestResource(t *testing.T) {
	tnnt, tnntErr := tenant.NewTenant("proj", "ns")
	assert.Nil(t, tnntErr)

	t.Run("Validate", func(t *testing.T) {
		invalidSpec := map[string]any{
			"description": map[string]any{"some": "desc"},
		}
		specWithoutValues := map[string]any{"a": "b"}
		t.Run("returns error for unknown kind", func(t *testing.T) {
			res := resource.Resource{}
			err := res.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: unknown kind")
		})
		t.Run("for view", func(t *testing.T) {
			t.Run("returns error when cannot decode view spec", func(t *testing.T) {
				res, err := resource.NewResource("proj.set.view_name1", resource.KindView, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				assert.Equal(t, "proj.set.view_name1", res.FullName())

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
					"'description' expected type 'string', got unconvertible type 'map[string]interface {}', value: "+
					"'map[some:desc]': not able to decode spec for proj.set.view_name1")
			})
			t.Run("returns error for validation failure", func(t *testing.T) {
				res, err := resource.NewResource("proj.set.view_name1", resource.KindView, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				assert.Equal(t, "proj.set.view_name1", res.FullName())

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource_view: view query is empty "+
					"for proj.set.view_name1")
			})
		})
		t.Run("for external_table", func(t *testing.T) {
			t.Run("returns error when cannot decode spec", func(t *testing.T) {
				res, err := resource.NewResource("proj.set.external_name1", resource.KindExternalTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				assert.Equal(t, "proj.set.external_name1", res.FullName())

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
					"'description' expected type 'string', got unconvertible type 'map[string]interface {}', value: "+
					"'map[some:desc]': not able to decode spec for proj.set.external_name1")
			})
			t.Run("returns error when external_table spec is invalid", func(t *testing.T) {
				res, err := resource.NewResource("proj.set.external_name1", resource.KindExternalTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				assert.Equal(t, "proj.set.external_name1", res.FullName())

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource_external_table: invalid schema "+
					"for proj.set.external_name1")
			})
		})
		t.Run("for table", func(t *testing.T) {
			t.Run("returns error when cannot decode table", func(t *testing.T) {
				res, err := resource.NewResource("proj.set.table_name1", resource.KindTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				assert.Equal(t, "proj.set.table_name1", res.FullName())

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
					"'description' expected type 'string', got unconvertible type 'map[string]interface {}', value: "+
					"'map[some:desc]': not able to decode spec for proj.set.table_name1")
			})
			t.Run("returns error when cannot decode table", func(t *testing.T) {
				res, err := resource.NewResource("proj.set.table_name1", resource.KindTable, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				assert.Equal(t, "proj.set.table_name1", res.FullName())

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource_table: empty schema for table proj.set.table_name1")
			})
		})
		t.Run("for dataset", func(t *testing.T) {
			t.Run("returns error when cannot decode dataset", func(t *testing.T) {
				res, err := resource.NewResource("proj.set_name1", resource.KindDataset, resource.Bigquery,
					tnnt, &resource.Metadata{}, invalidSpec)
				assert.Nil(t, err)

				err = res.Validate()
				assert.NotNil(t, err)
				assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
					"'description' expected type 'string', got unconvertible type 'map[string]interface {}', value: "+
					"'map[some:desc]': not able to decode spec for proj.set_name1")
			})
			t.Run("returns no error when validation passes", func(t *testing.T) {
				res, err := resource.NewResource("proj.set_name1", resource.KindDataset, resource.Bigquery,
					tnnt, &resource.Metadata{}, specWithoutValues)
				assert.Nil(t, err)

				err = res.Validate()
				assert.Nil(t, err)
			})
		})
	})

	t.Run("Equal", func(t *testing.T) {
		t.Run("returns false if current resource is nil", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			validResource, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			var nilResource *resource.Resource

			actualEquality := nilResource.Equal(validResource)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if incoming resource is nil", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			validResource, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			var nilResource *resource.Resource

			actualEquality := validResource.Equal(nilResource)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if current resource is invalid", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			validResource, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			invalidResource := &resource.Resource{}

			actualEquality := invalidResource.Equal(validResource)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if incoming resource is invalid", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			validResource, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			invalidResource := &resource.Resource{}

			actualEquality := validResource.Equal(invalidResource)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if name is not the same", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			resource1, err := resource.NewResource("project.dataset.table1", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table2", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if full name is not the same", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			resource1, err := resource.NewResource("project.dataset1.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset2.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if urn or dataset is not the same", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			resource1, err := resource.NewResource("project.dataset1.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset2.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			// current implementation does not provide different kind of store to explicitly produce such inequality

			actualEquality := resource1.Equal(resource2)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if metadata is not the same", func(t *testing.T) {
			metadata1 := &resource.Metadata{
				Version:     1,
				Description: "metadata 1 for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			metadata2 := &resource.Metadata{
				Version:     1,
				Description: "metadata 2 for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			resource1, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata1, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata2, spec)
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if status is not the same", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			resource1, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			err = resource1.MarkSkipped()
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.False(t, actualEquality)
		})
		t.Run("returns false if spec is not the same", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec1 := map[string]any{
				"description": "spec 1 for unit test",
			}
			spec2 := map[string]any{
				"description": "spec 2 for unit test",
			}
			resource1, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec1)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec2)
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.False(t, actualEquality)
		})
		t.Run("returns true if both current and incoming resources are nil", func(t *testing.T) {
			var resource1, resource2 *resource.Resource

			actualEquality1 := resource1.Equal(resource2)
			assert.True(t, actualEquality1)
			actualEquality2 := resource2.Equal(resource1)
			assert.True(t, actualEquality2)
		})
		t.Run("returns true if no additional difference is found", func(t *testing.T) {
			metadata := &resource.Metadata{
				Version:     1,
				Description: "metadata for unit test",
				Labels: map[string]string{
					"orcherstrator": "optimus",
				},
			}
			spec := map[string]any{
				"description": "spec for unit test",
			}
			resource1, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.True(t, actualEquality)
		})
	})

	t.Run("MarkStatus", func(t *testing.T) {
		meta := &resource.Metadata{Version: 1}
		spec := map[string]any{"abc": "def"}

		t.Run("MarkToCreate", func(t *testing.T) {
			t.Run("returns error if current status is not unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkSkipped()
				assert.NoError(t, err)

				actualError := res.MarkToCreate()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})

			t.Run("changes status to to_create and returns nil if current status is unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkToCreate()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusToCreate, res.Status())
			})
		})

		t.Run("MarkToUpdate", func(t *testing.T) {
			t.Run("changes status and returns nil if current status is unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkToUpdate()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusToUpdate, res.Status())
			})

			t.Run("changes status and returns nil if current status is success", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkToCreate()
				assert.NoError(t, err)
				err = res.MarkSuccess()
				assert.NoError(t, err)

				actualError := res.MarkToUpdate()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusToUpdate, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkSkipped()
				assert.NoError(t, err)

				actualError := res.MarkToUpdate()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkSkipped", func(t *testing.T) {
			t.Run("returns error if current status is not unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkToCreate()
				assert.NoError(t, err)

				actualError := res.MarkSkipped()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusToCreate, res.Status())
			})

			t.Run("changes status to skipped and returns error if current status is unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkSkipped()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkSuccess", func(t *testing.T) {
			meta := &resource.Metadata{Version: 1}
			spec := map[string]any{"abc": "def"}

			t.Run("changes status and return nil if current status is to_create", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkToCreate()
				assert.NoError(t, err)

				actualError := res.MarkSuccess()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusSuccess, res.Status())
			})

			t.Run("changes status and return nil if current status is to_update", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkToUpdate()
				assert.NoError(t, err)

				actualError := res.MarkSuccess()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusSuccess, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkSuccess()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusUnknown, res.Status())
			})
		})

		t.Run("MarkFailed", func(t *testing.T) {
			meta := &resource.Metadata{Version: 1}
			spec := map[string]any{"abc": "def"}

			t.Run("changes status to validation_failure and return nil if current status is unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkFailed()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusValidationFailure, res.Status())
			})

			t.Run("changes status to create_failure and return nil if current status is to_create", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkToCreate()
				assert.NoError(t, err)

				actualError := res.MarkFailed()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusCreateFailure, res.Status())
			})

			t.Run("changes status to update_failure and return nil if current status is to_update", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkToUpdate()
				assert.NoError(t, err)

				actualError := res.MarkFailed()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusUpdateFailure, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				err = res.MarkSkipped()
				assert.NoError(t, err)

				actualError := res.MarkFailed()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})
	})
}
