package service

import (
	"context"
	"fmt"

	"github.com/odpf/salt/log"

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
	CreateOrUpdateAll(ctx context.Context, resources []*resource.Resource) error
}

type ResourceManager interface {
	CreateResource(ctx context.Context, res *resource.Resource) error
	UpdateResource(ctx context.Context, res *resource.Resource) error
	BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error
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

// TODO: refactor this function in a way to utilize only one logger to handle logging to multiple places, such as server log as well as client log
func (rs ResourceService) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource) error {
	multiError := errors.NewMultiError("error validating resources")
	for _, r := range resources {
		if err := r.Validate(); err != nil {
			msg := fmt.Sprintf("error validating [%s]: %s", r.FullName(), err)
			multiError.Append(errors.Wrap(resource.EntityResource, msg, err))
			rs.logger.Error(msg)
		}
	}

	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store)
	if err != nil {
		multiError.Append(err)
		rs.logger.Error("error reading all existing resources: %s", err)
	}

	existingMappedByFullName := rs.getResourcesMappedByFullName(existingResources)
	resourcesToBatchUpdate := rs.getResourcesToBatchUpdate(resources, existingMappedByFullName)
	if len(resourcesToBatchUpdate) == 0 {
		rs.logger.Warn("no resources to be batch updated")
	}

	multiError.Append(rs.batch.CreateOrUpdateAll(ctx, resourcesToBatchUpdate))
	multiError.Append(rs.mgr.BatchUpdate(ctx, store, resourcesToBatchUpdate))
	return errors.MultiToError(multiError)
}

func (rs ResourceService) getResourcesToBatchUpdate(incomings []*resource.Resource, existingMappedByFullName map[string]*resource.Resource) []*resource.Resource {
	var output []*resource.Resource
	for _, incoming := range incomings {
		if existing, ok := existingMappedByFullName[incoming.FullName()]; ok {
			if incoming.Equal(existing) && existing.Status() == resource.StatusSuccess {
				incoming.MarkSkipped()

				rs.logger.Warn("resource [%s] is skipped because it has no changes", existing.FullName())
			} else {
				incoming.MarkToUpdate()
				output = append(output, incoming)

				rs.logger.Info("resource [%s] will be updated", existing.FullName())
			}
		} else {
			incoming.MarkToCreate()
			output = append(output, incoming)

			rs.logger.Info("resource [%s] will be created", incoming.FullName())
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
