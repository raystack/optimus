package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/resource/service"
	"github.com/odpf/optimus/core/tenant"
)

func TestResourceService(t *testing.T) {
	ctx := context.Background()
	tnnt, err := tenant.NewTenant("project_test", "namespace_tes")
	assert.NoError(t, err)
	meta := &resource.Metadata{
		Version:     1,
		Description: "test metadata",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{
		"description": "test spec",
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("sets status to create_failure and returns error if error is encountered when getting tenant", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, errors.New("unknown error"))

			actualError := rscService.Create(ctx, incoming)
			assert.ErrorContains(t, actualError, "unknown error")
			assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
		})

		t.Run("sets status to create_failure and returns error if resource is invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			spec := map[string]any{
				"spec": "invalid value",
			}
			invalid, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			tnntDetailsGetter.On("GetDetails", ctx, invalid.Tenant()).Return(nil, nil)

			repo.On("ReadByFullName", ctx, invalid.Tenant(), invalid.Dataset().Store, invalid.FullName()).Return(nil, errors.New("not found"))

			actualError := rscService.Create(ctx, invalid)
			assert.ErrorContains(t, actualError, "error validating resource")
			assert.Equal(t, resource.StatusCreateFailure, invalid.Status())
		})

		t.Run("resource does not exist in repository", func(t *testing.T) {
			t.Run("sets status to create_failure and returns error if status in store cannot be checked", func(t *testing.T) {
				repo := NewResourceRepository(t)
				batch := NewResourceBatchRepo(t)
				mgr := NewResourceManager(t)
				tnntDetailsGetter := NewTenantDetailsGetter(t)
				logger := log.NewLogrus()
				rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

				incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
				assert.NoError(t, err)

				tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

				repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(nil, errors.New("not found"))

				mgr.On("Exist", ctx, incoming).Return(false, errors.New("unknown error"))

				actualError := rscService.Create(ctx, incoming)
				assert.ErrorContains(t, actualError, "error checking resource")
				assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
			})

			t.Run("resource exists in store", func(t *testing.T) {
				t.Run("sets status to create_failure and returns error", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(nil, errors.New("not found"))

					mgr.On("Exist", ctx, incoming).Return(true, nil)

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "does not exist in Optimus but already exist in store")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})
			})

			t.Run("resource does not exist in store", func(t *testing.T) {
				t.Run("sets status to create_failure and returns error if error is encountered when creating to repository", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(nil, errors.New("not found"))

					mgr.On("Exist", ctx, incoming).Return(false, nil)

					repo.On("Create", ctx, incoming).Return(errors.New("unknown error"))

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "unknown error")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})

				t.Run("sets status to create_failure and returns error if error is encountered when creating to store", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(nil, errors.New("not found"))

					mgr.On("Exist", ctx, incoming).Return(false, nil)

					repo.On("Create", ctx, incoming).Return(nil)

					mgr.On("CreateResource", ctx, incoming).Return(errors.New("unknown error"))

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "unknown error")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})

				t.Run("sets status to create_failure and returns error if resource is successfully created but failed to update status to repository", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(nil, errors.New("not found"))

					mgr.On("Exist", ctx, incoming).Return(false, nil)

					repo.On("Create", ctx, incoming).Return(nil)

					mgr.On("CreateResource", ctx, incoming).Return(nil)

					repo.On("UpdateStatus", ctx, incoming).Return(errors.New("unknown error"))

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "unknown error")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})

				t.Run("sets status to success and exist in store to true and returns nil if resource is successfully created", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(nil, errors.New("not found"))

					mgr.On("Exist", ctx, incoming).Return(false, nil)

					repo.On("Create", ctx, incoming).Return(nil)

					mgr.On("CreateResource", ctx, incoming).Return(nil)

					repo.On("UpdateStatus", ctx, incoming).Return(nil)

					actualError := rscService.Create(ctx, incoming)
					assert.NoError(t, actualError)
					assert.Equal(t, resource.StatusSuccess, incoming.Status())
				})
			})
		})

		t.Run("resource exists in repository", func(t *testing.T) {
			t.Run("status exist_in_store is true", func(t *testing.T) {
				t.Run("sets status to create_failure and returns error", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)
					existing.MarkExistInStore()
					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "already exist in Optimus and in store")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})
			})

			t.Run("status exist_in_store is false", func(t *testing.T) {
				t.Run("sets status to create_failure and returns error if status in store cannot be checked", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)
					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

					mgr.On("Exist", ctx, existing).Return(false, errors.New("unknown error"))

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "error checking resource")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})

				t.Run("sets status to create_failure and returns error if resource exist in store", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)
					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					tnntDetailsGetter.On("GetDetails", ctx, tnnt).Return(nil, nil)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

					mgr.On("Exist", ctx, existing).Return(true, nil)

					actualError := rscService.Create(ctx, incoming)
					assert.ErrorContains(t, actualError, "already exist in Optimus and in store")
					assert.Equal(t, resource.StatusCreateFailure, incoming.Status())
				})
			})
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("sets status to update_failure and returns error if error is encountered when getting from repository", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			resourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, resourceToUpdate.FullName()).Return(nil, errors.New("unknown error"))

			actualError := rscService.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "unknown error")
			assert.Equal(t, resource.StatusUpdateFailure, resourceToUpdate.Status())
		})

		t.Run("sets status to update_failure and returns error if resource is invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			invalidSpec := map[string]any{
				"spec": "invalid value",
			}
			invalidIncoming, err := resource.NewResource("project.dataset.table", resource.KindTable, resource.Bigquery, tnnt, meta, invalidSpec)
			assert.NoError(t, err)
			validExisting, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, invalidIncoming.Tenant(), invalidIncoming.Dataset().Store, invalidIncoming.FullName()).Return(validExisting, nil)

			actualError := rscService.Update(ctx, invalidIncoming)
			assert.ErrorContains(t, actualError, "error validating resource")
			assert.Equal(t, resource.StatusUpdateFailure, invalidIncoming.Status())
		})

		t.Run("resource exists in repository", func(t *testing.T) {
			t.Run("validation fail", func(t *testing.T) {
				t.Run("sets status to update_failure and returns error if status exists in store is false and error is encountered when checking in store", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					updatedSpec := map[string]any{
						"description": "updated spec for update test",
					}
					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, updatedSpec)
					assert.NoError(t, err)
					existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

					mgr.On("Exist", ctx, existing).Return(false, errors.New("unknown error"))

					actualError := rscService.Update(ctx, incoming)
					assert.ErrorContains(t, actualError, "error checking resource")
					assert.Equal(t, resource.StatusUpdateFailure, incoming.Status())
				})

				t.Run("sets status to update_failure and returns error if status exists in store is false and resource does not exist in store", func(t *testing.T) {
					repo := NewResourceRepository(t)
					batch := NewResourceBatchRepo(t)
					mgr := NewResourceManager(t)
					tnntDetailsGetter := NewTenantDetailsGetter(t)
					logger := log.NewLogrus()
					rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

					updatedSpec := map[string]any{
						"description": "updated spec for update test",
					}
					incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, updatedSpec)
					assert.NoError(t, err)
					existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
					assert.NoError(t, err)

					repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

					mgr.On("Exist", ctx, existing).Return(false, nil)

					actualError := rscService.Update(ctx, incoming)
					assert.ErrorContains(t, actualError, "is not found in store")
					assert.Equal(t, resource.StatusUpdateFailure, incoming.Status())
				})

			})

			t.Run("validation success", func(t *testing.T) {
				t.Run("regards status exists in store", func(t *testing.T) {
					t.Run("sets status to update_failure and returns error if status exists in store is true and error is encountered when updating to repository", func(t *testing.T) {
						repo := NewResourceRepository(t)
						batch := NewResourceBatchRepo(t)
						mgr := NewResourceManager(t)
						tnntDetailsGetter := NewTenantDetailsGetter(t)
						logger := log.NewLogrus()
						rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

						updatedSpec := map[string]any{
							"description": "updated spec for update test",
						}
						incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, updatedSpec)
						assert.NoError(t, err)
						existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
						assert.NoError(t, err)
						existing.MarkExistInStore()

						repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

						repo.On("Update", ctx, incoming).Return(errors.New("unknown error"))

						actualError := rscService.Update(ctx, incoming)
						assert.ErrorContains(t, actualError, "unknown error")
						assert.Equal(t, resource.StatusUpdateFailure, incoming.Status())
					})
				})

				t.Run("regardless status exists in store", func(t *testing.T) {
					t.Run("sets status to update_failure and returns error if error is encountered when updating to store", func(t *testing.T) {
						repo := NewResourceRepository(t)
						batch := NewResourceBatchRepo(t)
						mgr := NewResourceManager(t)
						tnntDetailsGetter := NewTenantDetailsGetter(t)
						logger := log.NewLogrus()
						rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

						updatedSpec := map[string]any{
							"description": "updated spec for update test",
						}
						incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, updatedSpec)
						assert.NoError(t, err)
						existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
						assert.NoError(t, err)
						existing.MarkExistInStore()

						repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

						repo.On("Update", ctx, incoming).Return(nil)

						mgr.On("UpdateResource", ctx, incoming).Return(errors.New("unknown error"))

						actualError := rscService.Update(ctx, incoming)
						assert.ErrorContains(t, actualError, "unknown error")
						assert.Equal(t, resource.StatusUpdateFailure, incoming.Status())
					})

					t.Run("sets status to update_failure and returns error if resource is successfully updated but error is encountered when updating status to repository", func(t *testing.T) {
						repo := NewResourceRepository(t)
						batch := NewResourceBatchRepo(t)
						mgr := NewResourceManager(t)
						tnntDetailsGetter := NewTenantDetailsGetter(t)
						logger := log.NewLogrus()
						rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

						updatedSpec := map[string]any{
							"description": "updated spec for update test",
						}
						incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, updatedSpec)
						assert.NoError(t, err)
						existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
						assert.NoError(t, err)
						existing.MarkExistInStore()

						repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

						repo.On("Update", ctx, incoming).Return(nil)

						mgr.On("UpdateResource", ctx, incoming).Return(nil)

						repo.On("UpdateStatus", ctx, incoming).Return(errors.New("unknown error"))

						actualError := rscService.Update(ctx, incoming)
						assert.ErrorContains(t, actualError, "unknown error")
						assert.Equal(t, resource.StatusUpdateFailure, incoming.Status())
					})

					t.Run("sets status to success and returns nil if resource is successfully updated", func(t *testing.T) {
						repo := NewResourceRepository(t)
						batch := NewResourceBatchRepo(t)
						mgr := NewResourceManager(t)
						tnntDetailsGetter := NewTenantDetailsGetter(t)
						logger := log.NewLogrus()
						rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

						updatedSpec := map[string]any{
							"description": "updated spec for update test",
						}
						incoming, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, updatedSpec)
						assert.NoError(t, err)
						existing, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
						assert.NoError(t, err)
						existing.MarkExistInStore()

						repo.On("ReadByFullName", ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()).Return(existing, nil)

						repo.On("Update", ctx, incoming).Return(nil)

						mgr.On("UpdateResource", ctx, incoming).Return(nil)

						repo.On("UpdateStatus", ctx, incoming).Return(nil)

						actualError := rscService.Update(ctx, incoming)
						assert.NoError(t, actualError)
						assert.Equal(t, resource.StatusSuccess, incoming.Status())
					})
				})
			})
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("returns nil and error if resource name is empty", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			store := resource.Bigquery
			fullName := ""

			actualResource, actualError := rscService.Get(ctx, tnnt, store, fullName)
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "empty resource full name")
		})

		t.Run("returns nil and error if error is encountered when getting from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			fullName := "project.dataset"

			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName).Return(nil, errors.New("unknown error"))

			actualResource, actualError := rscService.Get(ctx, tnnt, resource.Bigquery, fullName)
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			fullName := "project.dataset"
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.Bigquery, fullName).Return(existingResource, nil)

			actualResource, actualError := rscService.Get(ctx, tnnt, resource.Bigquery, fullName)
			assert.EqualValues(t, existingResource, actualResource)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns nil and error if error is encountered when getting all from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return(nil, errors.New("unknown error"))

			actualResources, actualError := rscService.GetAll(ctx, tnnt, resource.Bigquery)
			assert.Nil(t, actualResources)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns resources and nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return([]*resource.Resource{existingResource}, nil)

			actualResources, actualError := rscService.GetAll(ctx, tnnt, resource.Bigquery)
			assert.EqualValues(t, []*resource.Resource{existingResource}, actualResources)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Deploy", func(t *testing.T) {
		t.Run("returns error if one or more resources are invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			validResourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			invalidResourceToUpdate := &resource.Resource{}
			resourcesToUpdate := []*resource.Resource{validResourceToUpdate, invalidResourceToUpdate}

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return([]*resource.Resource{}, nil)

			batch.On("CreateOrUpdateAll", ctx, []*resource.Resource{validResourceToUpdate}).Return(nil)

			mgr.On("Deploy", ctx, resource.Bigquery, []*resource.Resource{validResourceToUpdate}).Return(nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, resourcesToUpdate)
			assert.Error(t, actualError)
		})

		t.Run("returns error if error is encountered when reading from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			incomingResourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			resourcesToUpdate := []*resource.Resource{incomingResourceToUpdate}

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return(nil, errors.New("unknown error"))

			batch.On("CreateOrUpdateAll", ctx, resourcesToUpdate).Return(nil)

			mgr.On("Deploy", ctx, resource.Bigquery, resourcesToUpdate).Return(nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, resourcesToUpdate)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if there is no resource to create or modify", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			fullName := "project.dataset"
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("CreateOrUpdateAll", ctx, mock.Anything).Return(nil)

			mgr.On("Deploy", ctx, resource.Bigquery, mock.Anything).Return(nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate})
			assert.NoError(t, actualError)
		})

		t.Run("returns error if error is encountered when batch updating to repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			fullName := "project.dataset"
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("CreateOrUpdateAll", ctx, mock.Anything).Return(errors.New("unknown error"))

			mgr.On("Deploy", ctx, resource.Bigquery, mock.Anything).Return(nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate})
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when batch updating to store", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			fullName := "project.dataset"
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("CreateOrUpdateAll", ctx, mock.Anything).Return(nil)

			mgr.On("Deploy", ctx, resource.Bigquery, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate})

			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			tnntDetailsGetter := NewTenantDetailsGetter(t)
			logger := log.NewLogrus()
			rscService := service.NewResourceService(repo, batch, mgr, tnntDetailsGetter, logger)

			fullName := "project.dataset"
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.Bigquery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.Bigquery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("CreateOrUpdateAll", ctx, mock.Anything).Return(nil)

			mgr.On("Deploy", ctx, resource.Bigquery, mock.Anything).Return(nil)

			actualError := rscService.Deploy(ctx, tnnt, resource.Bigquery, []*resource.Resource{incomingResourceToUpdate})

			assert.NoError(t, actualError)
		})
	})
}

type ResourceRepository struct {
	mock.Mock
}

func (_m *ResourceRepository) Create(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *ResourceRepository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	ret := _m.Called(ctx, tnnt, store)

	var r0 []*resource.Resource
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.Store) []*resource.Resource); ok {
		r0 = rf(ctx, tnnt, store)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*resource.Resource)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, resource.Store) error); ok {
		r1 = rf(ctx, tnnt, store)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *ResourceRepository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error) {
	ret := _m.Called(ctx, tnnt, store, fullName)

	var r0 *resource.Resource
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.Store, string) *resource.Resource); ok {
		r0 = rf(ctx, tnnt, store, fullName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*resource.Resource)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, resource.Store, string) error); ok {
		r1 = rf(ctx, tnnt, store, fullName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *ResourceRepository) Update(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *ResourceRepository) UpdateStatus(ctx context.Context, resources ...*resource.Resource) error {
	_va := make([]interface{}, len(resources))
	for _i := range resources {
		_va[_i] = resources[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, ...*resource.Resource) error); ok {
		r0 = rf(ctx, resources...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewResourceRepository interface {
	mock.TestingT
	Cleanup(func())
}

func NewResourceRepository(t mockConstructorTestingTNewResourceRepository) *ResourceRepository {
	mock := &ResourceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type ResourceBatchRepo struct {
	mock.Mock
}

func (_m *ResourceBatchRepo) CreateOrUpdateAll(ctx context.Context, resources []*resource.Resource) error {
	ret := _m.Called(ctx, resources)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*resource.Resource) error); ok {
		r0 = rf(ctx, resources)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewResourceBatchRepo interface {
	mock.TestingT
	Cleanup(func())
}

func NewResourceBatchRepo(t mockConstructorTestingTNewResourceBatchRepo) *ResourceBatchRepo {
	mock := &ResourceBatchRepo{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type ResourceManager struct {
	mock.Mock
}

func (_m *ResourceManager) CreateResource(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *ResourceManager) Deploy(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	ret := _m.Called(ctx, store, resources)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, resource.Store, []*resource.Resource) error); ok {
		r0 = rf(ctx, store, resources)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *ResourceManager) Exist(ctx context.Context, res *resource.Resource) (bool, error) {
	ret := _m.Called(ctx, res)

	var r0 bool
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) bool); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *resource.Resource) error); ok {
		r1 = rf(ctx, res)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *ResourceManager) UpdateResource(ctx context.Context, res *resource.Resource) error {
	ret := _m.Called(ctx, res)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *resource.Resource) error); ok {
		r0 = rf(ctx, res)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewResourceManager interface {
	mock.TestingT
	Cleanup(func())
}

func NewResourceManager(t mockConstructorTestingTNewResourceManager) *ResourceManager {
	mock := &ResourceManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

type TenantDetailsGetter struct {
	mock.Mock
}

func (_m *TenantDetailsGetter) GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error) {
	ret := _m.Called(ctx, tnnt)

	var r0 *tenant.WithDetails
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) *tenant.WithDetails); ok {
		r0 = rf(ctx, tnnt)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tenant.WithDetails)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant) error); ok {
		r1 = rf(ctx, tnnt)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewTenantDetailsGetter interface {
	mock.TestingT
	Cleanup(func())
}

func NewTenantDetailsGetter(t mockConstructorTestingTNewTenantDetailsGetter) *TenantDetailsGetter {
	mock := &TenantDetailsGetter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
