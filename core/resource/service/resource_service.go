package service

import (
	"context"
	"fmt"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/api/writer"
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

	createRequest := resource.FromExisting(res, resource.ReplaceStatus(resource.StatusToCreate))
	if err := rs.repo.Create(ctx, createRequest); err != nil {
		rs.logger.Error("error creating resource [%s]: %s", res.FullName(), err)
		return err
	}

	return rs.mgr.CreateResource(ctx, createRequest)
}

func (rs ResourceService) Update(ctx context.Context, res *resource.Resource) error {
	if err := res.Validate(); err != nil {
		rs.logger.Error("error validating resource [%s]: %s", res.FullName(), err)
		return err
	}

	existing, err := rs.repo.ReadByFullName(ctx, res.Tenant(), res.Dataset().Store, res.FullName())
	if err != nil {
		rs.logger.Error("error getting stored resource [%s]: %s", res.FullName(), err)
		return err
	}

	updateRequest := resource.FromExisting(existing, resource.ReplaceStatus(resource.StatusToUpdate))
	if err := rs.repo.Update(ctx, updateRequest); err != nil {
		rs.logger.Error("error updating stored resource [%s]: %s", res.FullName(), err)
		return err
	}

	return rs.mgr.UpdateResource(ctx, updateRequest)
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
func (rs ResourceService) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource, logWriter writer.LogWriter) error {
	multiError := errors.NewMultiError("error validating resources")
	for _, r := range resources {
		if err := r.Validate(); err != nil {
			msg := fmt.Sprintf("error validating [%s]: %s", r.FullName(), err)

			multiError.Append(errors.Wrap(resource.EntityResource, msg, err))

			rs.logger.Error(msg)
			logWriter.Write(writer.LogLevelError, msg)
		}
	}
	if err := errors.MultiToError(multiError); err != nil {
		return err
	}

	existingResources, err := rs.repo.ReadAll(ctx, tnnt, store)
	if err != nil {
		rs.logger.Error("error reading all existing resources: %s", err)
		return err
	}

	existingMappedByFullName := rs.getResourcesMappedByFullName(existingResources)
	resourcesToBatchUpdate := rs.getResourcesToBatchUpdate(resources, existingMappedByFullName, logWriter)
	if len(resourcesToBatchUpdate) == 0 {
		rs.logger.Warn("no resources to be batch updated")
		return nil
	}

	if err := rs.batch.CreateOrUpdateAll(ctx, resourcesToBatchUpdate); err != nil {
		msg := fmt.Sprintf("error creating or updating incoming resources: %s", err)
		rs.logger.Error(msg)
		logWriter.Write(writer.LogLevelError, msg)
		return err
	}

	return rs.mgr.BatchUpdate(ctx, store, resourcesToBatchUpdate)
}

func (rs ResourceService) getResourcesToBatchUpdate(incomings []*resource.Resource, existingMappedByFullName map[string]*resource.Resource, logWriter writer.LogWriter) []*resource.Resource {
	var output []*resource.Resource
	for _, in := range incomings {
		if existing, ok := existingMappedByFullName[in.FullName()]; ok {
			existingStatus := existing.Status()
			incoming := resource.FromExisting(in, resource.ReplaceStatus(existingStatus))
			if !incoming.Equal(existing) {
				resourceToUpdate := resource.FromExisting(incoming, resource.ReplaceStatus(resource.StatusToUpdate))
				output = append(output, resourceToUpdate)

				rs.logger.Info("resource [%s] will be updated", existing.FullName())
			} else {
				msg := fmt.Sprintf("resource [%s] is skipped because it has no changes", existing.FullName())
				rs.logger.Warn(msg)
				logWriter.Write(writer.LogLevelWarning, msg)
			}
		} else {
			resourceToCreate := resource.FromExisting(in, resource.ReplaceStatus(resource.StatusToCreate))
			output = append(output, resourceToCreate)

			rs.logger.Info("resource [%s] will be updated", resourceToCreate.FullName())
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
