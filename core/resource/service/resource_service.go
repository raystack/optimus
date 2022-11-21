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

func (rs ResourceService) Create(ctx context.Context, incoming *resource.Resource) error {
	if _, err := rs.tnntDetailsGetter.GetDetails(ctx, incoming.Tenant()); err != nil {
		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		rs.logger.Error("error getting tenant for resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	fullNameToExistingResource := make(map[string]*resource.Resource)

	if existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()); err == nil {
		fullNameToExistingResource[incoming.FullName()] = existing
	}

	if validateErr := rs.validateCreate(ctx, incoming, fullNameToExistingResource); validateErr != nil {
		rs.logger.Error("error validating resource [%s] for create: %s", incoming.FullName(), validateErr)

		me := errors.NewMultiError("errors on creating resource")
		if incoming.Status() == resource.StatusMarkExistInStore {
			incoming.MarkExistInStore()
			if updateErr := rs.repo.UpdateStatus(ctx, incoming); updateErr != nil {
				rs.logger.Error("error updating status for resource [%s]: %s", incoming.FullName(), updateErr)
				me.Append(updateErr)
			}
		}
		me.Append(validateErr)

		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		return errors.MultiToError(me)
	}

	if err := rs.repo.Create(ctx, incoming); err != nil {
		rs.logger.Error("error creating resource [%s] to repository: %s", incoming.FullName(), err)
		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		return err
	}

	if err := rs.mgr.CreateResource(ctx, incoming); err != nil {
		rs.logger.Error("error creating resource [%s] to store [%s]: %s", incoming.FullName(), incoming.Dataset().Store.String(), err)
		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		return err
	}

	incoming.ChangeStatusTo(resource.StatusSuccess)
	incoming.MarkExistInStore()
	if err := rs.repo.UpdateStatus(ctx, incoming); err != nil {
		rs.logger.Error("error updating status for resource [%s] to repository: %s", incoming.FullName(), err)
		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		return err
	}
	return nil
}

func (rs ResourceService) Update(ctx context.Context, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", res.FullName(), err)
		return err
	}
	res.ChangeStatusTo(resource.StatusToUpdate)

	if _, err := rs.repo.ReadByFullName(ctx, res.Tenant(), res.Dataset().Store, res.FullName()); err != nil {
		rs.logger.Error("error getting stored resource [%s]: %s", res.FullName(), err)
		return err
	}

	res.ChangeStatusTo(resource.StatusToUpdate)
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
				r.ChangeStatusTo(resource.StatusUpdateFailure)
			} else {
				r.ChangeStatusTo(resource.StatusToUpdate)
			}
		} else {
			if err := r.Validate(); err != nil {
				r.ChangeStatusTo(resource.StatusCreateFailure)
			} else {
				r.ChangeStatusTo(resource.StatusToCreate)
			}
		}
	}

	multiError.Append(rs.batch.CreateOrUpdateAll(ctx, resources))
	multiError.Append(rs.mgr.Deploy(ctx, store, resources))
	return errors.MultiToError(multiError)
}

func (rs ResourceService) validateCreate(ctx context.Context, incoming *resource.Resource, fullNameToExistingResource map[string]*resource.Resource) error {
	if err := incoming.Validate(); err != nil {
		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		return errors.Wrap(resource.EntityResource, "error validating resource", err)
	}

	if existing, ok := fullNameToExistingResource[incoming.FullName()]; ok {
		if existing.ExistInStore() {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("resource [%s] already exist in Optimus and in store [%s]", existing.FullName(), existing.Dataset().Store.String())
			return errors.AlreadyExists(resource.EntityResource, msg)
		}

		existInStore, err := rs.mgr.Exist(ctx, existing)
		if err != nil {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("error checking resource [%s] in store [%s]", existing.FullName(), existing.Dataset().Store.String())
			return errors.Wrap(resource.EntityResource, msg, err)
		}

		if existInStore {
			incoming.MarkExistInStore()

			msg := fmt.Sprintf("resource [%s] already exist in Optimus and in store [%s]", existing.FullName(), existing.Dataset().Store.String())
			return errors.AlreadyExists(resource.EntityResource, msg)
		}
	} else {
		existInStore, err := rs.mgr.Exist(ctx, incoming)
		if err != nil {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("error checking resource [%s] in store [%s]", incoming.FullName(), incoming.Dataset().Store.String())
			return errors.Wrap(resource.EntityResource, msg, err)
		}

		if existInStore {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("resource [%s] does not exist in Optimus but already exist in store [%s]", incoming.FullName(), incoming.Dataset().Store.String())
			return errors.AlreadyExists(resource.EntityResource, msg)
		}
	}

	incoming.ChangeStatusTo(resource.StatusToCreate)
	return nil
}

func (ResourceService) getResourcesMappedByFullName(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
