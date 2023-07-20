package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/raystack/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/resource/service"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/internal/errors"
)

func TestResourceManager(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("proj", "ns")
	meta := &resource.Metadata{Description: "test resource"}
	var store resource.Store = "snowflake"

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.CreateResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("return error when datastore return an error", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusCreateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, createRequest).Return(errors.InternalError("resource", "error in create", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in create resource:\n internal error for entity resource: error in create")
		})
		t.Run("return error when create fails and mark also fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusCreateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).
				Return(errors.NotFound("resource", "error in update"))
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, createRequest).Return(errors.InvalidArgument("res", "error in create"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in create resource:\n invalid argument for entity res: error in "+
				"create:\n not found for entity resource: error in update")
		})
		t.Run("marks the create exist_in_store if already exists on datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusExistInStore
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, createRequest).Return(errors.AlreadyExists("resource", "error in create"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NoError(t, err)
		})
		t.Run("creates the resource on the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, createRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.Nil(t, err)
		})
	})
	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.UpdateResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("return error when datastore return an error", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Update", ctx, updateRequest).Return(errors.InternalError("resource", "error in update", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update resource:\n internal error for entity resource: error in update")
		})
		t.Run("return error when update fails and mark also fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).
				Return(errors.NotFound("resource", "error in update"))
			defer repo.AssertExpectations(t)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Update", ctx, updateRequest).Return(errors.InvalidArgument("res", "error in update"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update resource:\n invalid argument for entity res: error in "+
				"update:\n not found for entity resource: error in update")
		})
		t.Run("updates the resource on the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Update", ctx, updateRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.Nil(t, err)
		})
	})
	t.Run("Validate", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			err = manager.Validate(updateRequest)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("returns response from the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			logger := log.NewLogrus()
			manager := service.NewResourceManager(nil, logger)

			storeService := new(mockDataStore)
			storeService.On("Validate", updateRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.Validate(updateRequest)
			assert.NoError(t, err)
		})
	})
	t.Run("URN", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			_, err = manager.GetURN(updateRequest)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("returns response from the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			logger := log.NewLogrus()
			manager := service.NewResourceManager(nil, logger)

			storeService := new(mockDataStore)
			storeService.On("GetURN", updateRequest).Return("snowflake://db.schema.table", nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			urn, err := manager.GetURN(updateRequest)
			assert.NoError(t, err)
			assert.Equal(t, "snowflake://db.schema.table", urn)
		})
	})
	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			err = manager.BatchUpdate(ctx, store, []*resource.Resource{updateRequest})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: data store service not "+
				"found for snowflake")
		})
		t.Run("return error when error in both batchUpdate and update status", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))
			batchReq := []*resource.Resource{updateRequest}

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			me := errors.NewMultiError("error in batch")
			me.Append(errors.InternalError("resource", "enable to update in data store", nil))
			repo.On("UpdateStatus", ctx, argMatcher).Return(me)
			defer repo.AssertExpectations(t)

			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			matcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if res[0].Name() == updateRequest.Name() {
					res[0].MarkFailure()
					return true
				}
				return false
			})
			me2 := errors.NewMultiError("error in db update")
			me.Append(errors.InternalError("resource", "enable to update state in db", nil))
			storeService := new(mockDataStore)
			storeService.On("BatchUpdate", ctx, matcher).Return(me2)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.BatchUpdate(ctx, store, batchReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in batch update:"+
				"\n internal error for entity resource: enable to update in data store:"+
				"\n internal error for entity resource: enable to update state in db")
		})
		t.Run("returns success when no error in updating the batch", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))
			batchReq := []*resource.Resource{updateRequest}
			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			matcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if res[0].Name() == updateRequest.Name() {
					res[0].MarkSuccess()
					return true
				}
				return false
			})
			storeService := new(mockDataStore)
			storeService.On("BatchUpdate", ctx, matcher).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.BatchUpdate(ctx, store, batchReq)
			assert.Nil(t, err)
		})
	})
	t.Run("Backup", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			_, err = manager.Backup(ctx, backup, []*resource.Resource{res})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: data store service not found "+
				"for snowflake")
		})
		t.Run("runs backup in datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			logger := log.NewLogrus()
			manager := service.NewResourceManager(nil, logger)

			storeService := new(mockDataStore)
			storeService.On("Backup", ctx, backup, []*resource.Resource{res}).Return(&resource.BackupResult{
				ResourceNames: []string{"proj.ds.name1"},
			}, nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			result, err := manager.Backup(ctx, backup, []*resource.Resource{res})
			assert.NoError(t, err)
			assert.Equal(t, "proj.ds.name1", result.ResourceNames[0])
		})
	})
	t.Run("SyncResource", func(t *testing.T) {
		t.Run("returns error when store name is invalid", func(t *testing.T) {
			repo := new(mockRepo)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: datastore [snowflake] for resource [proj.ds.name1] is not found")
		})
		t.Run("returns error when create fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, res).Return(errors.InternalError("resource", "error in create", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: unable to create on datastore: "+
				"internal error for entity resource: error in create")
		})
		t.Run("returns error when update fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, res).Return(errors.AlreadyExists(resource.EntityResource, "table already exists"))
			storeService.On("Update", ctx, res).Return(errors.InternalError("resource", "error in update", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: unable to update on datastore: "+
				"internal error for entity resource: error in update")
		})
		t.Run("returns error when fails to update in db", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(errors.InternalError(resource.EntityResource, "error", nil))
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, res).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource: unable to update status in database: "+
				"internal error for entity resource: error")
		})
		t.Run("returns success when successful", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", "table", store, tnnt, meta, spec)
			assert.Nil(t, err)

			argMatcher := mock.MatchedBy(func(r []*resource.Resource) bool {
				if len(r) != 1 {
					return false
				}
				return r[0].Name() == res.Name()
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", ctx, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", ctx, res).Return(errors.AlreadyExists(resource.EntityResource, "table already exists"))
			storeService.On("Update", ctx, res).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.SyncResource(ctx, res)
			assert.Nil(t, err)
		})
	})
}

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) UpdateStatus(ctx context.Context, res ...*resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

type mockDataStore struct {
	mock.Mock
}

func (m *mockDataStore) Create(ctx context.Context, r *resource.Resource) error {
	return m.Called(ctx, r).Error(0)
}

func (m *mockDataStore) Update(ctx context.Context, r *resource.Resource) error {
	return m.Called(ctx, r).Error(0)
}

func (m *mockDataStore) BatchUpdate(ctx context.Context, resources []*resource.Resource) error {
	return m.Called(ctx, resources).Error(0)
}

func (m *mockDataStore) Validate(r *resource.Resource) error {
	return m.Called(r).Error(0)
}

func (m *mockDataStore) GetURN(r *resource.Resource) (string, error) {
	args := m.Called(r)
	return args.Get(0).(string), args.Error(1)
}

func (m *mockDataStore) Backup(ctx context.Context, backup *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	args := m.Called(ctx, backup, resources)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resource.BackupResult), args.Error(1)
}
