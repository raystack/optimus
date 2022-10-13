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

type ResourceManager interface {
	SyncToStore(context.Context, tenant.Tenant, resource.Name) error
}

type ResourceService struct {
	repo  ResourceRepository
	batch ResourceBatchRepo
	mgr   ResourceManager
}

func NewResourceService(repo ResourceRepository, batch ResourceBatchRepo, mgr ResourceManager) *ResourceService {
	return &ResourceService{
		repo:  repo,
		batch: batch,
		mgr:   mgr,
	}
}

func (rs ResourceService) Create(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		return err
	}

	createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))
	if err := rs.repo.Create(ctx, tnnt, createRequest); err != nil {
		return err
	}
	return rs.mgr.SyncToStore(ctx, tnnt, res.Name())
}

func (rs ResourceService) Update(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		return err
	}

	existing, err := rs.repo.Get(ctx, tnnt, res.Dataset().Store, res.Name())
	if err != nil {
		return err
	}

	updateRequest := resource.FromExisting(existing,
		resource.ReplaceKind(res.Kind()),
		resource.ReplaceDataset(res.Dataset()),
		resource.ReplaceTenant(res.Tenant()),
		resource.ReplaceSpec(res.Spec()),
		resource.ReplaceMetadata(res.Metadata()),
		resource.ReplaceStatus(resource.StatusToUpdate),
	)
	if err := rs.repo.Update(ctx, tnnt, updateRequest); err != nil {
		return err
	}
	return rs.mgr.SyncToStore(ctx, tnnt, res.Name())
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
