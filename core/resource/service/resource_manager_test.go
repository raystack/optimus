package service_test

import (
	"context"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/resource/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
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
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.CreateResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: data store service not found "+
				"for snowflake")
		})
		t.Run("return error when datastore return an error", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusCreateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", mock.Anything, createRequest).Return(errors.InternalError("resource", "error in create", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in create resource:\n internal error for entity resource: error in create")
		})
		t.Run("return error when create fails and mark also fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusCreateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).
				Return(errors.NotFound("resource", "error in update"))
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", mock.Anything, createRequest).Return(errors.InvalidArgument("res", "error in create"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in create resource:\n invalid argument for entity res: error in "+
				"create:\n not found for entity resource: error in update")
		})
		t.Run("marks the create success if already exists on datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", mock.Anything, createRequest).Return(errors.AlreadyExists("resource", "error in create"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.CreateResource(ctx, createRequest)
			assert.Nil(t, err)
		})
		t.Run("creates the resource on the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == createRequest.Name() && res[0].Status() == resource.StatusSuccess
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Create", mock.Anything, createRequest).Return(nil)
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
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)

			err = manager.UpdateResource(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: data store service not found "+
				"for snowflake")
		})
		t.Run("return error when datastore return an error", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(nil)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Update", mock.Anything, updateRequest).Return(errors.InternalError("resource", "error in update", nil))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update resource:\n internal error for entity resource: error in update")
		})
		t.Run("return error when update fails and mark also fails", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

			argMatcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if len(res) != 1 {
					return false
				}
				return res[0].Name() == updateRequest.Name() && res[0].Status() == resource.StatusUpdateFailure
			})
			repo := new(mockRepo)
			repo.On("UpdateStatus", mock.Anything, argMatcher).
				Return(errors.NotFound("resource", "error in update"))
			defer repo.AssertExpectations(t)
			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			storeService := new(mockDataStore)
			storeService.On("Update", mock.Anything, updateRequest).Return(errors.InvalidArgument("res", "error in update"))
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in update resource:\n invalid argument for entity res: error in "+
				"update:\n not found for entity resource: error in update")
		})
		t.Run("updates the resource on the datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
			assert.Nil(t, err)
			updateRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToUpdate))

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

			storeService := new(mockDataStore)
			storeService.On("Update", mock.Anything, updateRequest).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.UpdateResource(ctx, updateRequest)
			assert.Nil(t, err)
		})
	})
	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("return error when service not found for datastore", func(t *testing.T) {
			spec := map[string]any{"description": "test spec"}
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
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
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
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
			repo.On("UpdateStatus", mock.Anything, argMatcher).Return(me)
			defer repo.AssertExpectations(t)

			logger := log.NewLogrus()
			manager := service.NewResourceManager(repo, logger)

			matcher := mock.MatchedBy(func(res []*resource.Resource) bool {
				if res[0].Name() == updateRequest.Name() {
					res[0].MarkFailed()
					return true
				}
				return false
			})
			me2 := errors.NewMultiError("error in db update")
			me.Append(errors.InternalError("resource", "enable to update state in db", nil))
			storeService := new(mockDataStore)
			storeService.On("BatchUpdate", mock.Anything, matcher).Return(me2)
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
			res, err := resource.NewResource("proj.ds.name1", resource.KindTable, store, tnnt, meta, spec)
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
			storeService.On("BatchUpdate", mock.Anything, matcher).Return(nil)
			defer storeService.AssertExpectations(t)

			manager.RegisterDatastore(store, storeService)

			err = manager.BatchUpdate(ctx, store, batchReq)
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
