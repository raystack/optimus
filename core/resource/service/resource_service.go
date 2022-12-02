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

type ResourceService struct {
	repo              ResourceRepository
	mgr               ResourceManager
	tnntDetailsGetter TenantDetailsGetter

	logger log.Logger
}

func NewResourceService(logger log.Logger, repo ResourceRepository, mgr ResourceManager, tnntDetailsGetter TenantDetailsGetter) *ResourceService {
	return &ResourceService{
		repo:              repo,
		mgr:               mgr,
		tnntDetailsGetter: tnntDetailsGetter,
		logger:            logger,
	}
}

func (rs ResourceService) Create(ctx context.Context, incoming *resource.Resource) error {
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

		if _, err := rs.tnntDetailsGetter.GetDetails(ctx, incoming.Tenant()); err != nil {
			rs.logger.Error("error getting tenant for resource [%s] details: %s", incoming.FullName(), err)
			return err
		}
		incoming.MarkToCreate()

		if err := rs.repo.Create(ctx, incoming); err != nil {
			rs.logger.Error("error creating resource [%s] to db: %s", incoming.FullName(), err)
			return err
		}
	} else {
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
	return rs.mgr.CreateResource(ctx, incoming)
}

func (rs ResourceService) Update(ctx context.Context, incoming *resource.Resource) error {
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

	return rs.mgr.UpdateResource(ctx, incoming)
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
	for _, r := range resources {
		if err := rs.mgr.Validate(r); err != nil {
			msg := fmt.Sprintf("error validating [%s]: %s", r.FullName(), err)
			multiError.Append(errors.Wrap(resource.EntityResource, msg, err))

			rs.logger.Error(msg)
			r.MarkValidationFailure()
			continue
		}
		r.MarkValidationSuccess()

		urn, err := rs.mgr.GetURN(r)
		if err != nil {
			rs.logger.Error("error getting resource urn [%s]: %s", r.FullName(), err)
			return err
		}
		err = r.UpdateURN(urn)
		if err != nil {
			rs.logger.Error("error updating urn of resource [%s]: %s", r.FullName(), err)
			return err
		}
	}

	toUpdateOnStore, err := rs.getResourcesToBatchUpdate(ctx, tnnt, store, resources)
	multiError.Append(err)

	if len(toUpdateOnStore) == 0 {
		rs.logger.Warn("no resources to be batch updated")
		return errors.MultiToError(multiError)
	}

	multiError.Append(rs.mgr.BatchUpdate(ctx, store, toUpdateOnStore))
	return errors.MultiToError(multiError)
}

func (rs ResourceService) getResourcesToBatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, incomings []*resource.Resource) ([]*resource.Resource, error) {
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

func createFullNameToResourceMap(resources []*resource.Resource) map[string]*resource.Resource {
	output := make(map[string]*resource.Resource, len(resources))
	for _, r := range resources {
		output[r.FullName()] = r
	}
	return output
}
