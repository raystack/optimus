package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
)

type DataStore interface {
	Create(context.Context, *resource.Resource) error
	Update(context.Context, *resource.Resource) error
	BatchUpdate(context.Context, []*resource.Resource) error
}

type ResourceStatusRepo interface {
	MarkSuccess(ctx context.Context, res ...*resource.Resource) error
	MarkFailed(ctx context.Context, res ...*resource.Resource) error
}

type ResourceMgr struct {
	datastoreMap map[resource.Store]DataStore

	repo ResourceStatusRepo
}

func (m ResourceMgr) CreateResource(ctx context.Context, res *resource.Resource) error {
	datastore, ok := m.datastoreMap[res.Dataset().Store]
	if !ok {
		return nil // error about the datastore not found
	}

	err := datastore.Create(ctx, res)
	if err != nil {
		// if error is AlreadyExists mark as success
		statusErr := m.repo.MarkFailed(ctx, res)
		if statusErr != nil {
			return statusErr // Tell failed to mark as failed
		}
		return err
	}

	return m.repo.MarkSuccess(ctx, res)
}

func (m ResourceMgr) UpdateResource(ctx context.Context, res *resource.Resource) error {
	datastore, ok := m.datastoreMap[res.Dataset().Store]
	if !ok {
		return nil // error about the datastore not found
	}

	err := datastore.Update(ctx, res)
	if err != nil {
		statusErr := m.repo.MarkFailed(ctx, res)
		if statusErr != nil {
			return statusErr // Tell failed to mark as failed
		}
		return err
	}

	return m.repo.MarkSuccess(ctx, res)
}

func (m ResourceMgr) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return nil // error about the datastore not found
	}

	_ = datastore.BatchUpdate(ctx, resources)

	return m.repo.MarkSuccess(ctx, resources...)
}
