package service

import (
	"context"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

type DataStore interface {
	Create(context.Context, *resource.Resource) error
	Update(context.Context, *resource.Resource) error
	BatchUpdate(context.Context, []*resource.Resource) error
	Exist(context.Context, *resource.Resource) (bool, error)
}

type ResourceStatusRepo interface {
	UpdateStatus(ctx context.Context, res ...*resource.Resource) error
}

type ResourceMgr struct {
	datastoreMap map[resource.Store]DataStore

	repo ResourceStatusRepo

	logger log.Logger
}

func (m *ResourceMgr) CreateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		m.logger.Error("datastore [%s] for resource [%s] is not found", res.Dataset().Store.String(), res.FullName())
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	me := errors.NewMultiError("error in create resource")

	err := datastore.Create(ctx, res)
	if err != nil && !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		me.Append(err)
		res.ChangeStatusTo(resource.StatusCreateFailure)
		m.logger.Error("error creating resource [%s] to datastore [%s]: %s", res.FullName(), res.Dataset().Store.String(), err)
	} else {
		res.ChangeStatusTo(resource.StatusSuccess)
	}

	me.Append(m.repo.UpdateStatus(ctx, res))
	return errors.MultiToError(me)
}

func (m *ResourceMgr) UpdateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		m.logger.Error("datastore [%s] for resource [%s] is not found", res.Dataset().Store.String(), res.FullName())
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	me := errors.NewMultiError("error in update resource")

	err := datastore.Update(ctx, res)
	if err != nil {
		me.Append(err)
		res.ChangeStatusTo(resource.StatusUpdateFailure)
		m.logger.Error("error updating resource [%s] to datastore [%s]: %s", res.FullName(), res.Dataset().Store.String(), err)
	} else {
		res.ChangeStatusTo(resource.StatusSuccess)
	}

	me.Append(m.repo.UpdateStatus(ctx, res))
	return errors.MultiToError(me)
}

func (m *ResourceMgr) Exist(ctx context.Context, res *resource.Resource) (bool, error) {
	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		m.logger.Error("datastore [%s] for resource [%s] is not found", res.Dataset().Store.String(), res.FullName())
		return false, errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}
	return datastore.Exist(ctx, res)
}

func (m *ResourceMgr) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	datastore, ok := m.datastoreMap[store]
	if !ok {
		m.logger.Error("datastore [%s]  is not found", store.String())
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	err := errors.NewMultiError("error in batch update")
	err.Append(datastore.BatchUpdate(ctx, resources))
	err.Append(m.repo.UpdateStatus(ctx, resources...))

	return errors.MultiToError(err)
}

func (m *ResourceMgr) RegisterDatastore(store resource.Store, dataStore DataStore) {
	m.datastoreMap[store] = dataStore
}

func NewResourceManager(repo ResourceStatusRepo, logger log.Logger) *ResourceMgr {
	return &ResourceMgr{
		repo:         repo,
		datastoreMap: map[resource.Store]DataStore{},
		logger:       logger,
	}
}
