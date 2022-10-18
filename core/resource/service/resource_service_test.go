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
		t.Run("returns error if resource is invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			invalidResource := &resource.Resource{}

			actualError := rscService.Create(ctx, invalidResource)
			assert.Error(t, actualError)
		})

		t.Run("returns error if error is encountered when creating to repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			incomingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("Create", ctx, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Create(ctx, incomingResource)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when creating to store", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			incomingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("Create", ctx, mock.Anything).Return(nil)

			mgr.On("CreateResource", ctx, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Create(ctx, incomingResource)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			rsc, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("Create", ctx, mock.Anything).Return(nil)

			mgr.On("CreateResource", ctx, mock.Anything).Return(nil)

			actualError := rscService.Create(ctx, rsc)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("returns error if resource is invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			invalidResource := &resource.Resource{}

			actualError := rscService.Update(ctx, invalidResource)
			assert.Error(t, actualError)
		})

		t.Run("returns error if error is encountered when getting from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.BigQuery, fullName).Return(nil, errors.New("unknown error"))

			actualError := rscService.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when updating to repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.BigQuery, fullName).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when updating to store", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.BigQuery, fullName).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(nil)

			mgr.On("UpdateResource", ctx, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.Update(ctx, resourceToUpdate)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			resourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.BigQuery, fullName).Return(existingResource, nil)
			repo.On("Update", ctx, mock.Anything).Return(nil)

			mgr.On("UpdateResource", ctx, mock.Anything).Return(nil)

			actualError := rscService.Update(ctx, resourceToUpdate)
			assert.NoError(t, actualError)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("returns nil and error if resource name is empty", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			store := resource.BigQuery
			fullName := ""

			actualResource, actualError := rscService.Get(ctx, tnnt, store, fullName)
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "empty resource full name")
		})

		t.Run("returns nil and error if error is encountered when getting from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"

			repo.On("ReadByFullName", ctx, tnnt, resource.BigQuery, fullName).Return(nil, errors.New("unknown error"))

			actualResource, actualError := rscService.Get(ctx, tnnt, resource.BigQuery, fullName)
			assert.Nil(t, actualResource)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns resource and nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadByFullName", ctx, tnnt, resource.BigQuery, fullName).Return(existingResource, nil)

			actualResource, actualError := rscService.Get(ctx, tnnt, resource.BigQuery, fullName)
			assert.EqualValues(t, existingResource, actualResource)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns nil and error if error is encountered when getting all from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return(nil, errors.New("unknown error"))

			actualResources, actualError := rscService.GetAll(ctx, tnnt, resource.BigQuery)
			assert.Nil(t, actualResources)
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns resources and nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			existingResource, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return([]*resource.Resource{existingResource}, nil)

			actualResources, actualError := rscService.GetAll(ctx, tnnt, resource.BigQuery)
			assert.EqualValues(t, []*resource.Resource{existingResource}, actualResources)
			assert.NoError(t, actualError)
		})
	})

	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("returns error if one or more resources are invalid", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			validResourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)
			invalidResourceToUpdate := &resource.Resource{}
			resourcesToUpdate := []*resource.Resource{validResourceToUpdate, invalidResourceToUpdate}

			actualError := rscService.BatchUpdate(ctx, tnnt, resource.BigQuery, resourcesToUpdate)
			assert.Error(t, actualError)
		})

		t.Run("returns error if error is encountered when reading from repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			incomingResourceToUpdate, err := resource.NewResource("project.dataset", resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return(nil, errors.New("unknown error"))

			actualError := rscService.BatchUpdate(ctx, tnnt, resource.BigQuery, []*resource.Resource{incomingResourceToUpdate})
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if there is no resource to create or modify", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, meta, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return([]*resource.Resource{existingResource}, nil)

			actualError := rscService.BatchUpdate(ctx, tnnt, resource.BigQuery, []*resource.Resource{incomingResourceToUpdate})
			assert.NoError(t, actualError)
		})

		t.Run("returns error if error is encountered when updating to repo", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("UpdateAll", ctx, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.BatchUpdate(ctx, tnnt, resource.BigQuery, []*resource.Resource{incomingResourceToUpdate})
			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns error if error is encountered when batch updating to store", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("UpdateAll", ctx, mock.Anything).Return(nil)

			mgr.On("BatchUpdate", ctx, resource.BigQuery, mock.Anything).Return(errors.New("unknown error"))

			actualError := rscService.BatchUpdate(ctx, tnnt, resource.BigQuery, []*resource.Resource{incomingResourceToUpdate})

			assert.ErrorContains(t, actualError, "unknown error")
		})

		t.Run("returns nil if no error is encountered", func(t *testing.T) {
			repo := NewResourceRepository(t)
			batch := NewResourceBatchRepo(t)
			mgr := NewResourceManager(t)
			rscService := service.NewResourceService(repo, batch, mgr)

			fullName := "project.dataset"
			existingMetadata := &resource.Metadata{
				Description: "existing resource metadata",
			}
			existingResource, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, existingMetadata, spec)
			assert.NoError(t, err)
			incomingMetadata := &resource.Metadata{
				Description: "incoming resource metadata",
			}
			incomingResourceToUpdate, err := resource.NewResource(fullName, resource.KindDataset, resource.BigQuery, tnnt, incomingMetadata, spec)
			assert.NoError(t, err)

			repo.On("ReadAll", ctx, tnnt, resource.BigQuery).Return([]*resource.Resource{existingResource}, nil)

			batch.On("UpdateAll", ctx, mock.Anything).Return(nil)

			mgr.On("BatchUpdate", ctx, resource.BigQuery, mock.Anything).Return(nil)

			actualError := rscService.BatchUpdate(ctx, tnnt, resource.BigQuery, []*resource.Resource{incomingResourceToUpdate})

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

func (_m *ResourceBatchRepo) UpdateAll(ctx context.Context, resources []*resource.Resource) error {
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

func (_m *ResourceManager) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	ret := _m.Called(ctx, store, resources)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, resource.Store, []*resource.Resource) error); ok {
		r0 = rf(ctx, store, resources)
	} else {
		r0 = ret.Error(0)
	}

	return r0
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
