package service

import (
	"context"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

type DataStore interface {
	Create(context.Context, *resource.Resource) error
	Update(context.Context, *resource.Resource) error
	BatchUpdate(context.Context, []*resource.Resource) error
}

type ResourceStatusRepo interface {
	UpdateStatus(ctx context.Context, store resource.Store, res ...*resource.Resource) error
}

type ResourceMgr struct {
	datastoreMap map[resource.Store]DataStore

	repo ResourceStatusRepo
}

func (m *ResourceMgr) CreateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	err := datastore.Create(ctx, res)
	if !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		res.MarkFailed()
	}
	res.MarkSuccess()

	return m.repo.UpdateStatus(ctx, store, res)
}

func (m *ResourceMgr) UpdateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	err := datastore.Update(ctx, res)
	if err != nil {
		res.MarkFailed()
	}
	res.MarkSuccess()

	return m.repo.UpdateStatus(ctx, store, res)
}

func (m *ResourceMgr) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	err := errors.NewMultiError("error in batch update")
	err.Append(datastore.BatchUpdate(ctx, resources))
	err.Append(m.repo.UpdateStatus(ctx, store, resources...))

	return errors.MultiToError(err)
}

func NewResourceManager(repo ResourceStatusRepo) *ResourceMgr {
	return &ResourceMgr{
		repo:         repo,
		datastoreMap: map[resource.Store]DataStore{},
	}
}

func (m *ResourceMgr) RegisterDatastore(store resource.Store, dataStore DataStore) {
	m.datastoreMap[store] = dataStore
}
