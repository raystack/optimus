package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/tenant"
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
			_, err := resource.NewResource("", "table", resource.Bigquery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "resource name is empty")
		})
		t.Run("returns error when dataset name is empty", func(t *testing.T) {
			_, err := resource.NewResource("", "dataset", resource.Bigquery, tnnt, nil, nil)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "resource name is empty")
		})
		t.Run("returns error when invalid resource metadata", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			_, err := resource.NewResource("proj.set.res_name", "table", resource.Bigquery, tnnt, nil, spec)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: empty resource metadata for proj.set.res_name")
		})
		t.Run("returns error when invalid resource metadata", func(t *testing.T) {
			meta := resource.Metadata{
				Version:     1,
				Description: "description",
			}
			_, err := resource.NewResource("proj.set.res_name", "table",
				resource.Bigquery, tnnt, &meta, nil)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: empty resource spec "+
				"for proj.set.res_name")
		})
	})
	t.Run("creates table resource successfully", func(t *testing.T) {
		meta := &resource.Metadata{
			Version:     1,
			Description: "description",
		}
		spec := map[string]any{"a": "b"}
		res, err := resource.NewResource("proj.set.res_name", "table",
			resource.Bigquery, tnnt, meta, spec)
		assert.Nil(t, err)
		err = res.UpdateURN("bigquery://proj:set.res_name")
		assert.Nil(t, err)

		assert.Equal(t, "proj.set.res_name", res.FullName())
		assert.Equal(t, "proj.set.res_name", res.Name().String())
		assert.Equal(t, "bigquery://proj:set.res_name", res.URN())
		assert.EqualValues(t, meta, res.Metadata())
		assert.Equal(t, 3, len(res.NameSections()))
		assert.Equal(t, "table", res.Kind())
		assert.EqualValues(t, tnnt, res.Tenant())
		assert.Equal(t, resource.Bigquery.String(), res.Store().String())
		assert.Equal(t, resource.StatusUnknown, res.Status())
		assert.EqualValues(t, spec, res.Spec())
	})
	t.Run("creates dataset object successfully", func(t *testing.T) {
		meta := &resource.Metadata{
			Version:     1,
			Description: "description",
		}
		spec := map[string]any{"a": "b"}
		res, err := resource.NewResource("proj.dataset", "dataset",
			resource.Bigquery, tnnt, meta, spec)
		assert.Nil(t, err)

		assert.Equal(t, "proj.dataset", res.FullName())
		assert.Equal(t, 2, len(res.NameSections()))
		assert.EqualValues(t, meta, res.Metadata())
		assert.Equal(t, "dataset", res.Kind())
		assert.EqualValues(t, tnnt, res.Tenant())
		assert.Equal(t, resource.StatusUnknown, res.Status())
		assert.EqualValues(t, spec, res.Spec())
	})
}

func TestResource(t *testing.T) {
	tnnt, tnntErr := tenant.NewTenant("proj", "ns")
	assert.Nil(t, tnntErr)

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
			validResource, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
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
			validResource, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
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
			validResource, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
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
			validResource, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
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
			resource1, err := resource.NewResource("project.dataset.table1", "table", resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table2", "table", resource.Bigquery, tnnt, metadata, spec)
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
			resource1, err := resource.NewResource("project.dataset1.table", "table", resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset2.table", "table", resource.Bigquery, tnnt, metadata, spec)
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
			resource1, err := resource.NewResource("project.dataset1.table", "table", resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset2.table", "table", resource.Bigquery, tnnt, metadata, spec)
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
			resource1, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata1, spec)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata2, spec)
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
			resource1, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec1)
			assert.NoError(t, err)
			resource2, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec2)
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
		t.Run("returns true regardless of status if no additional difference is found", func(t *testing.T) {
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
			resource1, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)
			resource1.MarkToCreate()
			resource2, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
			resource1.MarkToUpdate()
			assert.NoError(t, err)

			actualEquality := resource1.Equal(resource2)
			assert.True(t, actualEquality)
		})
	})

	t.Run("UpdateURN", func(t *testing.T) {
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

		t.Run("can update the urn when not present", func(t *testing.T) {
			validResource, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)

			urn := "bigquery://project:dataset.table"
			err = validResource.UpdateURN(urn)
			assert.NoError(t, err)
			assert.Equal(t, urn, validResource.URN())
		})
		t.Run("returns error when urn already present", func(t *testing.T) {
			validResource, err := resource.NewResource("project.dataset.table", "table", resource.Bigquery, tnnt, metadata, spec)
			assert.NoError(t, err)

			urn := "bigquery://project:dataset.table"
			err = validResource.UpdateURN(urn)
			assert.NoError(t, err)

			err = validResource.UpdateURN(urn)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "urn already present")
		})
	})

	t.Run("MarkStatus", func(t *testing.T) {
		meta := &resource.Metadata{Version: 1}
		spec := map[string]any{"abc": "def"}

		t.Run("MarkValidationSuccess", func(t *testing.T) {
			t.Run("returns error if current status is not unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSkipped))

				actualError := res.MarkValidationSuccess()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})

			t.Run("changes status to validation_success and returns nil if current status is unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkValidationSuccess()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusValidationSuccess, res.Status())
			})
		})

		t.Run("MarkValidationFailure", func(t *testing.T) {
			t.Run("returns error if current status is not unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSkipped))

				actualError := res.MarkValidationFailure()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})

			t.Run("changes status to validation_failure and returns nil if current status is unknown", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkValidationFailure()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusValidationFailure, res.Status())
			})
		})

		t.Run("MarkSkipped", func(t *testing.T) {
			t.Run("returns error if current status is not validation_success", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

				actualError := res.MarkSkipped()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusToCreate, res.Status())
			})

			t.Run("changes status to skipped and returns error if current status is validation_success", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusValidationSuccess))

				actualError := res.MarkSkipped()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkToCreate", func(t *testing.T) {
			t.Run("changes status to to_create and returns nil if current status is validation_success", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusValidationSuccess))

				actualError := res.MarkToCreate()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusToCreate, res.Status())
			})

			t.Run("returns error if current status is other statuses", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSkipped))

				actualError := res.MarkToCreate()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkToUpdate", func(t *testing.T) {
			t.Run("changes status to to_update and returns nil if current status is validation_success", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusValidationSuccess))

				actualError := res.MarkToUpdate()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusToUpdate, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSkipped))

				actualError := res.MarkToUpdate()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkExistInStore", func(t *testing.T) {
			t.Run("changes status to exist_in_store and return nil if current status is to_create", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

				actualError := res.MarkExistInStore()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusExistInStore, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSkipped))

				actualError := res.MarkExistInStore()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkFailure", func(t *testing.T) {
			t.Run("changes status to create_failure and return nil if current status is to_create", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

				actualError := res.MarkFailure()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusCreateFailure, res.Status())
			})

			t.Run("changes status to update_failure and return nil if current status is to_update", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

				actualError := res.MarkFailure()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusUpdateFailure, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusSkipped))

				actualError := res.MarkFailure()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusSkipped, res.Status())
			})
		})

		t.Run("MarkSuccess", func(t *testing.T) {
			t.Run("changes status and returns nil if current status is to_create", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

				actualError := res.MarkSuccess()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusSuccess, res.Status())
			})

			t.Run("changes status and returns nil if current status is to_update", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)
				res = resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

				actualError := res.MarkSuccess()
				assert.NoError(t, actualError)
				assert.Equal(t, resource.StatusSuccess, res.Status())
			})

			t.Run("returns error if other status", func(t *testing.T) {
				res, err := resource.NewResource("proj.ds.name1", "table", resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				actualError := res.MarkSuccess()
				assert.Error(t, actualError)
				assert.Equal(t, resource.StatusUnknown, res.Status())
			})
		})
	})
}
