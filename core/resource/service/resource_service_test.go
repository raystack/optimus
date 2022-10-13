package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/resource/service"
	"github.com/odpf/optimus/core/tenant"
)

func TestResourceService(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		t.Run("return error if resource is invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			rsc := &resource.Resource{}

			actualError := rscService.Create(ctx, tnnt, rsc)

			assert.Error(t, actualError)
		})

		t.Run("return error if encountering error when creating to repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			rsc, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("Create", ctx, tnnt, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Create(ctx, tnnt, rsc)

			assert.Error(t, actualError)
		})

		t.Run("return error if encountering error when synchronizing to store", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			rsc, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("Create", ctx, tnnt, mock.Anything).Return(nil)

			mgr.On("SyncToStore", ctx, tnnt, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Create(ctx, tnnt, rsc)

			assert.Error(t, actualError)
		})

		t.Run("return nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			rsc, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("Create", ctx, tnnt, mock.Anything).Return(nil)

			mgr.On("SyncToStore", ctx, tnnt, mock.Anything).Return(nil)

			actualError := rscService.Create(ctx, tnnt, rsc)

			assert.NoError(t, actualError)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("return error if resource is invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			resourceToUpdate := &resource.Resource{}

			actualError := rscService.Update(ctx, tnnt, resourceToUpdate)

			assert.Error(t, actualError)
		})

		t.Run("return error if error encountered when getting from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("ReadByName", ctx, tnnt, resource.BigQuery, mock.Anything).Return(nil, errors.New("unknown error"))

			actualError := rscService.Update(ctx, tnnt, resourceToUpdate)

			assert.Error(t, actualError)
		})

		t.Run("return error if error encountered when updating to repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("ReadByName", ctx, tnnt, resource.BigQuery, mock.Anything).Return(existingResource, nil)
			repo.On("Update", ctx, tnnt, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Update(ctx, tnnt, resourceToUpdate)

			assert.Error(t, actualError)
		})

		t.Run("return error if error encountered when synchronizing to store", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("ReadByName", ctx, tnnt, resource.BigQuery, mock.Anything).Return(existingResource, nil)
			repo.On("Update", ctx, tnnt, mock.Anything).Return(nil)

			mgr.On("SyncToStore", ctx, tnnt, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Update(ctx, tnnt, resourceToUpdate)

			assert.Error(t, actualError)
		})

		t.Run("return nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			resourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}
			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("ReadByName", ctx, tnnt, resource.BigQuery, mock.Anything).Return(existingResource, nil)
			repo.On("Update", ctx, tnnt, mock.Anything).Return(nil)

			mgr.On("SyncToStore", ctx, tnnt, mock.Anything).Return(nil)

			actualError := rscService.Update(ctx, tnnt, resourceToUpdate)

			assert.NoError(t, actualError)
		})
	})

	t.Run("Read", func(t *testing.T) {
		t.Run("return nil and error if resource name is empty", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			store := resource.BigQuery
			var resourceName resource.Name

			actualResource, actualError := rscService.Read(ctx, tnnt, store, resourceName)

			assert.Nil(t, actualResource)
			assert.Error(t, actualError)
		})

		t.Run("return nil and error if error encountered when getting from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			store := resource.BigQuery
			var resourceName resource.Name = "project.dataset"

			repo.On("ReadByName", ctx, tnnt, resource.BigQuery, mock.Anything).Return(nil, errors.New("unknown error"))

			actualResource, actualError := rscService.Read(ctx, tnnt, store, resourceName)

			assert.Nil(t, actualResource)
			assert.Error(t, actualError)
		})

		t.Run("return resource and nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			store := resource.BigQuery
			var resourceName resource.Name = "project.dataset"
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("ReadByName", ctx, tnnt, resource.BigQuery, mock.Anything).Return(existingResource, nil)

			actualResource, actualError := rscService.Read(ctx, tnnt, store, resourceName)

			assert.NotNil(t, actualResource)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetAll", func(t *testing.T) {
		t.Run("return nil and error if error encountered when getting all from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			store := resource.BigQuery

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return(nil, errors.New("unknown error"))

			actualResource, actualError := rscService.GetAll(ctx, tnnt, store)

			assert.Nil(t, actualResource)
			assert.Error(t, actualError)
		})

		t.Run("return resources and nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			ctx := context.Background()
			tnnt := tenant.Tenant{}
			store := resource.BigQuery
			meta := &resource.Metadata{}
			spec := map[string]any{
				"key": "value",
			}
			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			if err != nil {
				panic(err)
			}

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return([]*resource.Resource{existingResource}, nil)

			actualResource, actualError := rscService.GetAll(ctx, tnnt, store)

			assert.NotEmpty(t, actualResource)
			assert.NoError(t, actualError)
		})
	})
}

type ResourceRepository struct {
	mock.Mock
}

func (_m *ResourceRepository) Create(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	ret := _m.Called(ctx, tnnt, res)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, *resource.Resource) error); ok {
		r0 = rf(ctx, tnnt, res)
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

func (_m *ResourceRepository) ReadByName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, name resource.Name) (*resource.Resource, error) {
	ret := _m.Called(ctx, tnnt, store, name)

	var r0 *resource.Resource
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.Store, resource.Name) *resource.Resource); ok {
		r0 = rf(ctx, tnnt, store, name)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*resource.Resource)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, resource.Store, resource.Name) error); ok {
		r1 = rf(ctx, tnnt, store, name)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *ResourceRepository) Update(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	ret := _m.Called(ctx, tnnt, res)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, *resource.Resource) error); ok {
		r0 = rf(ctx, tnnt, res)
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

func (_m *ResourceBatchRepo) UpdateAll(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) error {
	ret := _m.Called(ctx, tnnt, resources)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, []*resource.Resource) error); ok {
		r0 = rf(ctx, tnnt, resources)
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

func (_m *ResourceManager) SyncToStore(_a0 context.Context, _a1 tenant.Tenant, _a2 resource.Name) error {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, resource.Name) error); ok {
		r0 = rf(_a0, _a1, _a2)
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
