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
	me := errors.NewMultiError("errors on creating resource")

	if _, err := rs.tnntDetailsGetter.GetDetails(ctx, incoming.Tenant()); err != nil {
		rs.logger.Error("error getting tenant for resource [%s]: %s", incoming.FullName(), err)

		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		me.Append(err)
		return errors.MultiToError(me)
	}

	fullNameToExistingResource := make(map[string]*resource.Resource)
	if existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName()); err == nil {
		fullNameToExistingResource[incoming.FullName()] = existing
	}

	if validateErr := rs.validateCreate(ctx, incoming, fullNameToExistingResource); validateErr != nil {
		rs.logger.Error("error validating resource [%s] for create: %s", incoming.FullName(), validateErr)

		if incoming.Status() == resource.StatusMarkExistInStore {
			if existing, ok := fullNameToExistingResource[incoming.FullName()]; ok {
				incoming.ChangeStatusTo(existing.Status())
			}
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
		me.Append(err)
		return errors.MultiToError(me)
	}

	if err := rs.mgr.CreateResource(ctx, incoming); err != nil {
		rs.logger.Error("error creating resource [%s] to store [%s]: %s", incoming.FullName(), incoming.Dataset().Store.String(), err)

		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		me.Append(err)
		return errors.MultiToError(me)
	}

	incoming.MarkExistInStore()
	incoming.ChangeStatusTo(resource.StatusSuccess)
	if err := rs.repo.UpdateStatus(ctx, incoming); err != nil {
		rs.logger.Error("error updating status for resource [%s] to repository: %s", incoming.FullName(), err)

		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		me.Append(err)
	}
	return errors.MultiToError(me)
}

func (rs ResourceService) Update(ctx context.Context, incoming *resource.Resource) error {
	me := errors.NewMultiError("errors on updating resource")

	existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Dataset().Store, incoming.FullName())
	if err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		rs.logger.Error("error getting stored resource [%s]: %s", incoming.FullName(), err)
		me.Append(err)
		return errors.MultiToError(me)
	}

	fullNameToExistingResource := map[string]*resource.Resource{
		existing.FullName(): existing,
	}

	if err := rs.validateUpdate(ctx, incoming, fullNameToExistingResource); err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		rs.logger.Error("error validating resource [%s] for update: %s", incoming.FullName(), err)
		me.Append(err)
		return errors.MultiToError(me)
	}

	if err := rs.repo.Update(ctx, incoming); err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		rs.logger.Error("error updating resource [%s] to repository: %s", incoming.FullName(), err)
		me.Append(err)
		return errors.MultiToError(me)
	}

	if err := rs.mgr.UpdateResource(ctx, incoming); err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		rs.logger.Error("error updating resource [%s] to store [%s]: %s", incoming.FullName(), incoming.Dataset().Store.String(), err)
		me.Append(err)
		return errors.MultiToError(me)
	}

	incoming.MarkExistInStore()
	incoming.ChangeStatusTo(resource.StatusSuccess)
	if err := rs.repo.UpdateStatus(ctx, incoming); err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		rs.logger.Error("error updating status for resource [%s]: %s", incoming.FullName(), err)
		me.Append(err)
	}
	return errors.MultiToError(me)
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

func (rs ResourceService) Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, incomings []*resource.Resource) error {
	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store)
	if err != nil {
		rs.logger.Error("error reading all existing resources: %s", err)
		return err
	}
	fullNameToExistingResource := rs.getResourcesMappedByFullName(existingResources)

	me := errors.NewMultiError("error batch updating resources")
	me.Append(rs.validateDeploy(ctx, incomings, fullNameToExistingResource))
	me.Append(rs.batch.CreateOrUpdateAll(ctx, incomings))
	me.Append(rs.mgr.Deploy(ctx, store, incomings))
	return errors.MultiToError(me)
}

func (rs ResourceService) validateDeploy(ctx context.Context, incomings []*resource.Resource, fullNameToExistingResource map[string]*resource.Resource) error {
	me := errors.NewMultiError("error batch updating resources")
	for _, incoming := range incomings {
		if _, alreadyExists := fullNameToExistingResource[incoming.FullName()]; alreadyExists {
			if validateErr := rs.validateUpdate(ctx, incoming, fullNameToExistingResource); validateErr != nil {
				incoming.ChangeStatusTo(resource.StatusUpdateFailure)

				rs.logger.Error("error validating resource [%s] for update: %s", incoming.FullName(), validateErr)
				me.Append(validateErr)
			}
		} else {
			if validateErr := rs.validateCreate(ctx, incoming, fullNameToExistingResource); validateErr != nil {
				rs.logger.Error("error validating resource [%s] for create: %s", incoming.FullName(), validateErr)
				me.Append(validateErr)
			}
		}
	}
	return errors.MultiToError(me)
}

func (rs ResourceService) validateUpdate(ctx context.Context, incoming *resource.Resource, fullNameToExistingResource map[string]*resource.Resource) error {
	me := errors.NewMultiError("errors on validating for update")

	if err := incoming.Validate(); err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)
		me.Append(errors.Wrap(resource.EntityResource, "error validating resource", err))
		return errors.MultiToError(me)
	}

	existing, ok := fullNameToExistingResource[incoming.FullName()]
	if !ok {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		msg := fmt.Sprintf("resource [%s] does not exist in Optimus", incoming.FullName())
		me.Append(errors.NotFound(resource.EntityResource, msg))
		return errors.MultiToError(me)
	}
	if existing.ExistInStore() {
		incoming.MarkExistInStore()
		incoming.ChangeStatusTo(resource.StatusToUpdate)
		return errors.MultiToError(me)
	}

	existInStore, err := rs.mgr.Exist(ctx, existing)
	if err != nil {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		msg := fmt.Sprintf("error checking resource [%s] in store [%s]", existing.FullName(), existing.Dataset().Store.String())
		me.Append(errors.Wrap(resource.EntityResource, msg, err))
		return errors.MultiToError(me)
	}

	if !existInStore {
		incoming.ChangeStatusTo(resource.StatusUpdateFailure)

		msg := fmt.Sprintf("resource [%s] is found in Optimus but not found in store [%s]", incoming.FullName(), incoming.Dataset().Store.String())
		me.Append(errors.NotFound(resource.EntityResource, msg))
		return errors.MultiToError(me)
	}

	incoming.MarkExistInStore()
	incoming.ChangeStatusTo(resource.StatusToUpdate)
	return errors.MultiToError(me)
}

func (rs ResourceService) validateCreate(ctx context.Context, incoming *resource.Resource, fullNameToExistingResource map[string]*resource.Resource) error {
	me := errors.NewMultiError("errors on validating for create")

	if err := incoming.Validate(); err != nil {
		incoming.ChangeStatusTo(resource.StatusCreateFailure)
		me.Append(errors.Wrap(resource.EntityResource, "error validating resource", err))
		return errors.MultiToError(me)
	}

	if existing, ok := fullNameToExistingResource[incoming.FullName()]; ok {
		if existing.ExistInStore() {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("resource [%s] already exist in Optimus and in store [%s]", existing.FullName(), existing.Dataset().Store.String())
			me.Append(errors.AlreadyExists(resource.EntityResource, msg))
			return errors.MultiToError(me)
		}

		existInStore, err := rs.mgr.Exist(ctx, existing)
		if err != nil {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("error checking resource [%s] in store [%s]", existing.FullName(), existing.Dataset().Store.String())
			me.Append(errors.Wrap(resource.EntityResource, msg, err))
			return errors.MultiToError(me)
		}

		if existInStore {
			incoming.MarkExistInStore()
			incoming.ChangeStatusTo(resource.StatusMarkExistInStore)

			msg := fmt.Sprintf("resource [%s] already exist in Optimus and in store [%s]", existing.FullName(), existing.Dataset().Store.String())
			me.Append(errors.AlreadyExists(resource.EntityResource, msg))
			return errors.MultiToError(me)
		}
	} else {
		existInStore, err := rs.mgr.Exist(ctx, incoming)
		if err != nil {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("error checking resource [%s] in store [%s]", incoming.FullName(), incoming.Dataset().Store.String())
			me.Append(errors.Wrap(resource.EntityResource, msg, err))
			return errors.MultiToError(me)
		}

		if existInStore {
			incoming.ChangeStatusTo(resource.StatusCreateFailure)

			msg := fmt.Sprintf("resource [%s] does not exist in Optimus but already exist in store [%s]", incoming.FullName(), incoming.Dataset().Store.String())
			me.Append(errors.AlreadyExists(resource.EntityResource, msg))
			return errors.MultiToError(me)
		}
	}

	incoming.ChangeStatusTo(resource.StatusToCreate)
	return errors.MultiToError(me)
}

func (ResourceService) getResourcesMappedByFullName(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
