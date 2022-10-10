package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type ResourceRepository interface {
	Create(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error
	Update(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error
	Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, name resource.Name) (*resource.Resource, error)
	GetAllFor(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
}

type ResourceBatchRepo interface {
	UpdateAll(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) error
}

type ResourceService struct {
	repo  ResourceRepository
	batch ResourceBatchRepo
}

func (rs ResourceService) Create(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	if res == nil {
		return errors.InvalidArgument(resource.EntityResource, "invalid resource to create")
	}

	if err := res.Validate(); err != nil {
		return err
	}
	// Save, will add the status as to_create, find if not already exists

	// If we keep it sync then call manager.Create() to create on datastore
	// and call repo.UpdateStatus(ctx, tnnt, res.Name, "success")
	// or res.SyncSuccess() and do a repo.update(...)
	return rs.repo.Create(ctx, tnnt, res)
}

func (rs ResourceService) Update(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	if res == nil {
		return errors.InvalidArgument(resource.EntityResource, "invalid resource to update")
	}

	if err := res.Validate(); err != nil {
		return err
	}

	// here do something like
	// dbRes := repo.Get(...)
	// res.isEqual(incoming) -- If status not success return false.
	//   return
	//

	// Check in repo if the spec is same, then return.
	return rs.repo.Update(ctx, tnnt, res)
}

func (rs ResourceService) Read(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceName resource.Name) (*resource.Resource, error) {
	if resourceName == "" {
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource name")
	}

	return rs.repo.Get(ctx, tnnt, store, resourceName)
}

func (rs ResourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	return rs.repo.GetAllFor(ctx, tnnt, store)
}

func (rs ResourceService) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource) error {
	// Here query all the resource for tenant and do the matches.
	_, err := rs.repo.GetAllFor(ctx, tnnt, store)
	if err != nil {
		return err
	}
	// do a loop over all the received resources, and find the one with same name or urn from db ones

	// Once identified all resources for create/update
	// Do a batch insert/update

	return rs.batch.UpdateAll(ctx, tnnt, resources)

	// resource manager -> will need to use rate limit
}
