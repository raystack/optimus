package service

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

type DataStore interface {
	Create(context.Context, *resource.Resource) error
	Update(context.Context, *resource.Resource) error
	BatchUpdate(context.Context, []*resource.Resource) error
	Backup(context.Context, *resource.Backup, []*resource.Resource) (*resource.BackupResult, error)
}

type ResourceStatusRepo interface {
	UpdateStatus(ctx context.Context, res ...*resource.Resource) error
}

type ResourceMgr struct {
	datastoreMap map[resource.Store]DataStore

	repo ResourceStatusRepo

	tracer trace.Tracer
}

func (m *ResourceMgr) CreateResource(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := m.tracer.Start(ctx, "CreateResource()")
	defer span.End()

	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	me := errors.NewMultiError("error in create resource")

	err := datastore.Create(spanCtx, res)
	if err != nil && !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		me.Append(err)
		me.Append(res.MarkFailed())
	} else {
		me.Append(res.MarkSuccess())
	}

	me.Append(m.repo.UpdateStatus(spanCtx, res))
	return errors.MultiToError(me)
}

func (m *ResourceMgr) UpdateResource(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := m.tracer.Start(ctx, "UpdateResource()")
	defer span.End()

	store := res.Dataset().Store
	datastore, ok := m.datastoreMap[store]
	if !ok {
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	me := errors.NewMultiError("error in update resource")

	err := datastore.Update(spanCtx, res)
	if err != nil {
		me.Append(err)
		me.Append(res.MarkFailed())
	} else {
		me.Append(res.MarkSuccess())
	}

	me.Append(m.repo.UpdateStatus(spanCtx, res))
	return errors.MultiToError(me)
}

func (m *ResourceMgr) BatchUpdate(ctx context.Context, store resource.Store, resources []*resource.Resource) error {
	spanCtx, span := m.tracer.Start(ctx, "BatchUpdate()")
	defer span.End()

	datastore, ok := m.datastoreMap[store]
	if !ok {
		return errors.InvalidArgument(resource.EntityResource, "data store service not found for "+store.String())
	}

	err := errors.NewMultiError("error in batch update")
	err.Append(datastore.BatchUpdate(spanCtx, resources))
	err.Append(m.repo.UpdateStatus(spanCtx, resources...))

	return errors.MultiToError(err)
}

func (m *ResourceMgr) Backup(ctx context.Context, details *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	datastore, ok := m.datastoreMap[details.Store()]
	if !ok {
		return nil, errors.InvalidArgument(resource.EntityResource, "data store service not found for "+details.Store().String())
	}

	return datastore.Backup(ctx, details, resources)
}

func NewResourceManager(repo ResourceStatusRepo) *ResourceMgr {
	return &ResourceMgr{
		repo:         repo,
		datastoreMap: map[resource.Store]DataStore{},
		tracer:       otel.Tracer("core.resource.service"),
	}
}

func (m *ResourceMgr) RegisterDatastore(store resource.Store, dataStore DataStore) {
	m.datastoreMap[store] = dataStore
}
