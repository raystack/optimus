package resource

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type Repository struct {
	db *gorm.DB

	tracer trace.Tracer
}

func NewRepository(db *gorm.DB) *Repository {
	return &Repository{
		db:     db,
		tracer: otel.Tracer("internal.store.postgres.resource.Repository{}"),
	}
}

func (r Repository) Create(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := r.tracer.Start(ctx, "Create()")
	defer span.End()

	incomingResource := fromResourceToModel(res)
	return r.create(r.db.WithContext(spanCtx), incomingResource)
}

func (r Repository) Update(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := r.tracer.Start(ctx, "Update()")
	defer span.End()

	incomingResource := fromResourceToModel(res)
	return r.update(r.db.WithContext(spanCtx), incomingResource)
}

func (r Repository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error) {
	spanCtx, span := r.tracer.Start(ctx, "ReadByFullName()")
	defer span.End()

	var namespaceName string
	if name, err := tnnt.NamespaceName(); err == nil {
		namespaceName = name.String()
	}
	res, err := r.readByFullName(r.db.WithContext(spanCtx), tnnt.ProjectName().String(), namespaceName, store.String(), fullName)
	if err != nil {
		return nil, err
	}
	return fromModelToResource(res)
}

func (r Repository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	spanCtx, span := r.tracer.Start(ctx, "ReadAll()")
	defer span.End()

	namespaceName, err := tnnt.NamespaceName()
	if err != nil {
		return nil, err
	}
	var resources []*Resource
	if err := r.db.WithContext(spanCtx).
		Where("project_name = ? and namespace_name = ? and store = ?",
			tnnt.ProjectName().String(), namespaceName.String(), store.String(),
		).Find(&resources).Error; err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error reading from database", err)
	}
	output := make([]*resource.Resource, len(resources))
	for i, res := range resources {
		m, err := fromModelToResource(res)
		if err != nil {
			return nil, err
		}
		output[i] = m
	}
	return output, nil
}

func (r Repository) CreateOrUpdateAll(ctx context.Context, resources []*resource.Resource) error {
	spanCtx, span := r.tracer.Start(ctx, "CreateOrUpdateAll()")
	defer span.End()

	resourceModels := make([]*Resource, len(resources))
	for i, res := range resources {
		resourceModels[i] = fromResourceToModel(res)
	}
	return r.db.WithContext(spanCtx).Transaction(func(tx *gorm.DB) error {
		multiErr := errors.NewMultiError("error updating resources status")
		for _, m := range resourceModels {
			if m.Status == resource.StatusToCreate.String() {
				if err := r.create(tx, m); err != nil {
					multiErr.Append(errors.Wrap(resource.EntityResource, "error creating resource to database", err))
				}
			} else if m.Status == resource.StatusToUpdate.String() {
				if err := r.update(tx, m); err != nil {
					multiErr.Append(errors.Wrap(resource.EntityResource, "error updating resource to database", err))
				}
			}
		}
		return errors.MultiToError(multiErr)
	})
}

func (r Repository) UpdateStatus(ctx context.Context, resources ...*resource.Resource) error {
	spanCtx, span := r.tracer.Start(ctx, "UpdateStatus()")
	defer span.End()

	resourceModels := make([]*Resource, len(resources))
	for i, res := range resources {
		resourceModels[i] = fromResourceToModel(res)
	}

	multiErr := errors.NewMultiError("error updating resources status")
	for _, m := range resourceModels {
		result := r.db.WithContext(spanCtx).Model(&Resource{}).
			Where("project_name = ?", m.ProjectName).
			Where("namespace_name = ?", m.NamespaceName).
			Where("store = ?", m.Store).
			Where("full_name = ?", m.FullName).
			Update("status", m.Status)
		if result.Error != nil {
			multiErr.Append(errors.Wrap(resource.EntityResource, "error updating status to database", result.Error))
		}
		if result.RowsAffected == 0 {
			multiErr.Append(errors.NotFound(resource.EntityResource, "resource is not found "+m.FullName))
		}
	}
	return errors.MultiToError(multiErr)
}

func (r Repository) update(db *gorm.DB, res *Resource) error {
	existingResource, err := r.readByFullName(db, res.ProjectName, res.NamespaceName, res.Store, res.FullName)
	if err != nil {
		return err
	}
	err = db.Where(existingResource).Updates(res).Error
	if err != nil {
		err = errors.Wrap(resource.EntityResource, "error updating resource to database", err)
	}
	return err
}

func (Repository) readByFullName(db *gorm.DB, projectName, namespaceName, store, fullName string) (*Resource, error) {
	var res *Resource
	query := "project_name = ? and store = ? and full_name = ?"
	if namespaceName != "" {
		query += " and namespace_name = ?"
	}
	if err := db.Where(query, projectName, store, fullName, namespaceName).First(&res).Error; err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error reading from database", err)
	}
	return res, nil
}

func (Repository) create(db *gorm.DB, m *Resource) error {
	if err := db.Create(m).Error; err != nil {
		return errors.Wrap(resource.EntityResource, "error creating resource to database", err)
	}
	return nil
}
