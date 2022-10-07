package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type ResourceRepository interface {
	Save(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error
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
	// Repo should check if the spec is same and not save
	return rs.repo.Save(ctx, tnnt, res)
}

func (rs ResourceService) Update(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	if res == nil {
		return errors.InvalidArgument(resource.EntityResource, "invalid resource to update")
	}

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

func (rs ResourceService) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) error {
	return rs.batch.UpdateAll(ctx, tnnt, resources)
}
