package service

import (
	"context"
	"fmt"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

type DataStore interface {
	Create(context.Context, *resource.Resource) error
	Update(context.Context, *resource.Resource) error
	BatchUpdate(context.Context, []*resource.Resource) error
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
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", res.Dataset().Store.String(), res.FullName())
		m.logger.Error(msg)
		return errors.InternalError(resource.EntityResource, msg, nil)
	}

	me := errors.NewMultiError("error in create resource")
	if err := datastore.Create(ctx, res); err != nil {
		m.logger.Error("error creating resource [%s] to datastore [%s]: %s", res.FullName(), res.Dataset().Store.String(), err)

		if errors.IsErrorType(err, errors.ErrAlreadyExists) {
			me.Append(res.MarkExistInStore())
		} else {
			me.Append(res.MarkFailure())
		}
		me.Append(err)
	} else {
		me.Append(res.MarkSuccess())
	}

	me.Append(m.repo.UpdateStatus(ctx, res))
	return errors.MultiToError(me)
}

func (m *ResourceMgr) UpdateResource(ctx context.Context, res *resource.Resource) error {
	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		msg := fmt.Sprintf("datastore [%s] for resource [%s] is not found", res.Dataset().Store.String(), res.FullName())
		m.logger.Error(msg)
		return errors.InternalError(resource.EntityResource, msg, nil)
	}

	me := errors.NewMultiError("error in update resource")
	if err := datastore.Update(ctx, res); err != nil {
		me.Append(err)
		me.Append(res.MarkFailure())
		m.logger.Error("error updating resource [%s] to datastore [%s]: %s", res.FullName(), res.Dataset().Store.String(), err)
	} else {
		me.Append(res.MarkSuccess())
	}

	me.Append(m.repo.UpdateStatus(ctx, res))
	return errors.MultiToError(me)
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
