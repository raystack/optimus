package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
)

type DataStore interface {
	Create(context.Context, tenant.Tenant, *resource.Resource) error
	Update(context.Context, tenant.Tenant, *resource.Resource) error
	BatchUpdate(context.Context, tenant.Tenant, []*resource.Resource) error
}

type ResourceStatusRepo interface {
	MarkSuccess(ctx context.Context, tnnt tenant.Tenant, res ...*resource.Resource) error
	MarkFailed(ctx context.Context, tnnt tenant.Tenant, res ...*resource.Resource) error
}

type ResourceManager struct {
	datastoreMap map[resource.Store]DataStore

	repo ResourceStatusRepo
}

func (m ResourceManager) CreateResource(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	datastore, ok := m.datastoreMap[res.Dataset().Store]
	if !ok {
		return nil // error about the datastore not found
	}

	err := datastore.Create(ctx, tnnt, res)
	if err != nil {
		// if error is AlreadyExists mark as success
		statusErr := m.repo.MarkFailed(ctx, tnnt, res)
		if statusErr != nil {
			return statusErr // Tell failed to mark as failed
		}
		return err
	}

	return m.repo.MarkSuccess(ctx, tnnt, res)
}

func (m ResourceManager) UpdateResource(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error {
	datastore, ok := m.datastoreMap[res.Dataset().Store]
	if !ok {
		return nil // error about the datastore not found
	}

	err := datastore.Update(ctx, tnnt, res)
	if err != nil {
		statusErr := m.repo.MarkFailed(ctx, tnnt, res)
		if statusErr != nil {
			return statusErr // Tell failed to mark as failed
		}
		return err
	}

	return m.repo.MarkSuccess(ctx, tnnt, res)
}

func (m ResourceManager) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource) error {
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return nil // error about the datastore not found
	}

	_ = datastore.BatchUpdate(ctx, tnnt, resources)

	return m.repo.MarkSuccess(ctx, tnnt, resources...)
}
