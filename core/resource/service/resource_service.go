package service

import (
	"context"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type ResourceRepository interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
	UpdateStatus(ctx context.Context, resources ...*resource.Resource) error
	ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error)
	ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
}

type ResourceBatchRepo interface {
	CreateOrUpdateAll(ctx context.Context, resources []*resource.Resource) error
}

type ResourceManager interface {
	CreateResource(ctx context.Context, res *resource.Resource) error
	UpdateResource(ctx context.Context, res *resource.Resource) error
	Deploy(ctx context.Context, store resource.Store, resources []*resource.Resource) error

	Exist(ctx context.Context, res *resource.Resource) (bool, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
}

type ResourceService struct {
	repo              ResourceRepository
	batch             ResourceBatchRepo
	mgr               ResourceManager
	tnntDetailsGetter TenantDetailsGetter

	logger log.Logger
}

func NewResourceService(repo ResourceRepository, batch ResourceBatchRepo, mgr ResourceManager, tnntDetailsGetter TenantDetailsGetter, logger log.Logger) *ResourceService {
	return &ResourceService{
		repo:              repo,
		batch:             batch,
		mgr:               mgr,
		tnntDetailsGetter: tnntDetailsGetter,
		logger:            logger,
	}
}

func (rs ResourceService) Create(ctx context.Context, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", res.FullName(), err)
		return err
	}
	res.MarkToCreate()

	if _, err := rs.tnntDetailsGetter.GetDetails(ctx, res.Tenant()); err != nil {
		rs.logger.Error("error getting tenant details: %s", err)
		return err
	}

	res.MarkToCreate()
	if err := rs.repo.Create(ctx, res); err != nil {
		rs.logger.Error("error creating resource [%s]: %s", res.FullName(), err)
		return err
	}

	return rs.mgr.CreateResource(ctx, res)
}

func (rs ResourceService) Update(ctx context.Context, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", res.FullName(), err)
		return err
	}
	res.MarkToUpdate()

	if _, err := rs.repo.ReadByFullName(ctx, res.Tenant(), res.Dataset().Store, res.FullName()); err != nil {
		rs.logger.Error("error getting stored resource [%s]: %s", res.FullName(), err)
		return err
	}

	res.MarkToUpdate()
	if err := rs.repo.Update(ctx, res); err != nil {
		rs.logger.Error("error updating stored resource [%s]: %s", res.FullName(), err)
		return err
	}

	return rs.mgr.UpdateResource(ctx, res)
}

func (rs ResourceService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceFullName string) (*resource.Resource, error) {
	if resourceFullName == "" {
		rs.logger.Error("resource full name is empty")
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource full name")
	}
	return rs.repo.ReadByFullName(ctx, tnnt, store, resourceFullName)
}

func (rs ResourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	return rs.repo.ReadAll(ctx, tnnt, store)
}

func (rs ResourceService) Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource) error {
	multiError := errors.NewMultiError("error batch updating resources")

	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store)
	if err != nil {
		multiError.Append(err)
		rs.logger.Error("error reading all existing resources: %s", err)
	}

	existingMappedByFullName := rs.getResourcesMappedByFullName(existingResources)

	for _, r := range resources {
		if _, alreadyExist := existingMappedByFullName[r.FullName()]; alreadyExist {
			if err := r.Validate(); err != nil {
				r.MarkUpdateFailure()
			} else {
				r.MarkToUpdate()
			}
		} else {
			if err := r.Validate(); err != nil {
				r.MarkCreateFailure()
			} else {
				r.MarkToCreate()
			}
		}
	}

	multiError.Append(rs.batch.CreateOrUpdateAll(ctx, resources))
	multiError.Append(rs.mgr.Deploy(ctx, store, resources))
	return errors.MultiToError(multiError)
}

func (ResourceService) getResourcesMappedByFullName(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
