package service

import (
	"context"
	"fmt"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type ResourceRepository interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
	ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error)
	ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
}

type ResourceManager interface {
	CreateResource(ctx context.Context, res *resource.Resource) error
	UpdateResource(ctx context.Context, res *resource.Resource) error
	BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error
	Validate(res *resource.Resource) error
	GetURN(res *resource.Resource) (string, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
}

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type ResourceService struct {
	repo ResourceRepository
	mgr  ResourceManager

	logger       log.Logger
	eventHandler EventHandler
}

func NewResourceService(logger log.Logger, repo ResourceRepository, mgr ResourceManager, eventHandler EventHandler) *ResourceService {
	return &ResourceService{
		repo:         repo,
		mgr:          mgr,
		logger:       logger,
		eventHandler: eventHandler,
	}
}

func (rs ResourceService) Create(ctx context.Context, incoming *resource.Resource) error { // nolint:gocritic
	if err := rs.mgr.Validate(incoming); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	incoming.MarkValidationSuccess()
	urn, err := rs.mgr.GetURN(incoming)
	if err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	err = incoming.UpdateURN(urn)
	if err != nil {
		rs.logger.Error("error updating urn of resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	if existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName()); err != nil {
		if !errors.IsErrorType(err, errors.ErrNotFound) {
			rs.logger.Error("error getting resource [%s]: %s", incoming.FullName(), err)
			return err
		}
		incoming.MarkToCreate()

		if err := rs.repo.Create(ctx, incoming); err != nil {
			rs.logger.Error("error creating resource [%s] to db: %s", incoming.FullName(), err)
			return err
		}
	} else {
		if existing.Status() == resource.StatusSuccess || existing.Status() == resource.StatusExistInStore {
			return nil // Note: return in case resource already exists
		}
		if !resource.StatusForToCreate(existing.Status()) {
			msg := fmt.Sprintf("cannot create resource [%s] since it already exists with status [%s]", incoming.FullName(), existing.Status())
			rs.logger.Error(msg)
			return errors.InvalidArgument(resource.EntityResource, msg)
		}
		incoming.MarkToCreate()

		if err := rs.repo.Update(ctx, incoming); err != nil {
			rs.logger.Error("error updating resource [%s] to db: %s", incoming.FullName(), err)
			return err
		}
	}

	if err := rs.mgr.CreateResource(ctx, incoming); err != nil {
		return err
	}

	rs.raiseCreateEvent(incoming)
	return nil
}

func (rs ResourceService) Update(ctx context.Context, incoming *resource.Resource) error { // nolint:gocritic
	if err := rs.mgr.Validate(incoming); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	incoming.MarkValidationSuccess()
	urn, err := rs.mgr.GetURN(incoming)
	if err != nil {
		rs.logger.Error("error validating resource [%s]: %s", incoming.FullName(), err)
		return err
	}
	err = incoming.UpdateURN(urn)
	if err != nil {
		rs.logger.Error("error updating urn of resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	existing, err := rs.repo.ReadByFullName(ctx, incoming.Tenant(), incoming.Store(), incoming.FullName())
	if err != nil {
		rs.logger.Error("error getting stored resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	if !(resource.StatusForToUpdate(existing.Status())) {
		msg := fmt.Sprintf("cannot update resource [%s] with existing status [%s]", incoming.FullName(), existing.Status())
		rs.logger.Error(msg)
		return errors.InvalidArgument(resource.EntityResource, msg)
	}
	incoming.MarkToUpdate()

	if err := rs.repo.Update(ctx, incoming); err != nil {
		rs.logger.Error("error updating stored resource [%s]: %s", incoming.FullName(), err)
		return err
	}

	if err := rs.mgr.UpdateResource(ctx, incoming); err != nil {
		return err
	}
	rs.raiseUpdateEvent(incoming)
	return nil
}

func (rs ResourceService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceFullName string) (*resource.Resource, error) { // nolint:gocritic
	if resourceFullName == "" {
		rs.logger.Error("resource full name is empty")
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource full name")
	}
	return rs.repo.ReadByFullName(ctx, tnnt, store, resourceFullName)
}

func (rs ResourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) { // nolint:gocritic
	return rs.repo.ReadAll(ctx, tnnt, store)
}

func (rs ResourceService) Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource) error { // nolint:gocritic
	multiError := errors.NewMultiError("error batch updating resources")
	for _, r := range resources {
		if err := rs.mgr.Validate(r); err != nil {
			msg := fmt.Sprintf("error validating [%s]: %s", r.FullName(), err)
			multiError.Append(errors.Wrap(resource.EntityResource, msg, err))

			rs.logger.Error(msg)
			r.MarkValidationFailure()
			continue
		}

		urn, err := rs.mgr.GetURN(r)
		if err != nil {
			multiError.Append(err)
			rs.logger.Error("error getting resource urn [%s]: %s", r.FullName(), err)
			continue
		}
		err = r.UpdateURN(urn)
		if err != nil {
			multiError.Append(err)
			rs.logger.Error("error updating urn of resource [%s]: %s", r.FullName(), err)
			continue
		}
		r.MarkValidationSuccess()
	}

	toUpdateOnStore, err := rs.getResourcesToBatchUpdate(ctx, tnnt, store, resources)
	multiError.Append(err)

	if len(toUpdateOnStore) == 0 {
		rs.logger.Warn("no resources to be batch updated")
		return errors.MultiToError(multiError)
	}

	var toCreate []*resource.Resource
	var toUpdate []*resource.Resource
	for _, r := range toUpdateOnStore {
		if r.Status() == resource.StatusToCreate {
			toCreate = append(toCreate, r)
		} else if r.Status() == resource.StatusToUpdate {
			toUpdate = append(toUpdate, r)
		}
	}

	multiError.Append(rs.mgr.BatchUpdate(ctx, store, toUpdateOnStore))

	for _, r := range toCreate {
		rs.raiseCreateEvent(r)
	}

	for _, r := range toUpdate {
		rs.raiseUpdateEvent(r)
	}

	return errors.MultiToError(multiError)
}

func (rs ResourceService) getResourcesToBatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, incomings []*resource.Resource) ([]*resource.Resource, error) { // nolint:gocritic
	existingResources, readErr := rs.repo.ReadAll(ctx, tnnt, store)
	if readErr != nil {
		rs.logger.Error("error reading all existing resources: %s", readErr)
		return nil, readErr
	}

	existingMappedByFullName := createFullNameToResourceMap(existingResources)

	var toUpdateOnStore []*resource.Resource
	me := errors.NewMultiError("error in resources to batch update")

	for _, incoming := range incomings {
		if incoming.Status() != resource.StatusValidationSuccess {
			continue
		}

		existing, ok := existingMappedByFullName[incoming.FullName()]
		if !ok {
			_ = incoming.MarkToCreate()
			err := rs.repo.Create(ctx, incoming)
			if err == nil {
				toUpdateOnStore = append(toUpdateOnStore, incoming)
			}
			me.Append(err)
			continue
		}

		if resource.StatusIsSuccess(existing.Status()) && incoming.Equal(existing) {
			_ = incoming.MarkSkipped()
			rs.logger.Warn("resource [%s] is skipped because it has no changes", existing.FullName())
			continue
		}

		if resource.StatusForToCreate(existing.Status()) {
			_ = incoming.MarkToCreate()
		} else if resource.StatusForToUpdate(existing.Status()) {
			_ = incoming.MarkToUpdate()
		}

		err := rs.repo.Update(ctx, incoming)
		if err == nil {
			toUpdateOnStore = append(toUpdateOnStore, incoming)
		}
		me.Append(err)
	}
	return toUpdateOnStore, errors.MultiToError(me)
}

func (rs ResourceService) raiseCreateEvent(res *resource.Resource) { // nolint:gocritic
	if res.Status() != resource.StatusSuccess {
		return
	}

	ev, err := event.NewResourceCreatedEvent(res)
	if err != nil {
		rs.logger.Error("error creating event for resource create: %s", err)
		return
	}
	rs.eventHandler.HandleEvent(ev)
}

func (rs ResourceService) raiseUpdateEvent(res *resource.Resource) { // nolint:gocritic
	if res.Status() != resource.StatusSuccess {
		return
	}

	ev, err := event.NewResourceUpdatedEvent(res)
	if err != nil {
		rs.logger.Error("error creating event for resource update: %s", err)
		return
	}
	rs.eventHandler.HandleEvent(ev)
}

func createFullNameToResourceMap(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
