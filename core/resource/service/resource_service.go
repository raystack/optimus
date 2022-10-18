package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type ResourceRepository interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
	ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error)
	ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
}

type ResourceBatchRepo interface {
	UpdateAll(ctx context.Context, resources []*resource.Resource) error
}

type ResourceManager interface {
	CreateResource(ctx context.Context, res *resource.Resource) error
	UpdateResource(ctx context.Context, res *resource.Resource) error
	BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error
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

func (rs ResourceService) Create(ctx context.Context, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		return err
	}

	createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))
	if err := rs.repo.Create(ctx, createRequest); err != nil {
		return err
	}

	return rs.mgr.CreateResource(ctx, createRequest)
}

func (rs ResourceService) Update(ctx context.Context, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		return err
	}

	existing, err := rs.repo.ReadByFullName(ctx, res.Tenant(), res.Dataset().Store, res.FullName())
	if err != nil {
		return err
	}

	updateRequest := resource.FromExisting(existing, resource.ReplaceStatus(resource.StatusToUpdate))
	if err := rs.repo.Update(ctx, updateRequest); err != nil {
		return err
	}

	return rs.mgr.UpdateResource(ctx, updateRequest)
}

func (rs ResourceService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceName string) (*resource.Resource, error) {
	if resourceName == "" {
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource full name")
	}
	return rs.repo.ReadByFullName(ctx, tnnt, store, resourceName)
}

func (rs ResourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	return rs.repo.ReadAll(ctx, tnnt, store)
}

func (rs ResourceService) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource) error {
	for _, r := range resources {
		if err := r.Validate(); err != nil {
			return err
		}
	}

	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store)
	if err != nil {
		return err
	}

	existingMappedByFullName := rs.getResourcesMappedByFullName(existingResources)
	resourcesToBatchUpdate := rs.getResourcesToBatchUpdate(resources, existingMappedByFullName)
	if len(resourcesToBatchUpdate) == 0 {
		return nil
	}

	if err := rs.batch.UpdateAll(ctx, resourcesToBatchUpdate); err != nil {
		return err
	}

	return rs.mgr.BatchUpdate(ctx, store, resourcesToBatchUpdate)
}

func (ResourceService) getResourcesToBatchUpdate(incomings []*resource.Resource, existingMappedByFullName map[string]*resource.Resource) []*resource.Resource {
	var output []*resource.Resource
	for _, in := range incomings {
		if existing, ok := existingMappedByFullName[in.FullName()]; ok {
			existingStatus := existing.Status()
			incoming := resource.FromExisting(in, resource.ReplaceStatus(existingStatus))
			if !incoming.Equal(existing) {
				resourceToUpdate := resource.FromExisting(incoming, resource.ReplaceStatus(resource.StatusToUpdate))
				output = append(output, resourceToUpdate)
			}
		} else {
			resourceToCreate := resource.FromExisting(in, resource.ReplaceStatus(resource.StatusToCreate))
			output = append(output, resourceToCreate)
		}
	}
	return output
}

func (ResourceService) getResourcesMappedByFullName(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
