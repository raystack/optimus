package resource

import (
	"context"

	"gorm.io/gorm"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository {
	return &Repository{
		db: db,
	}
}

func (r Repository) Create(ctx context.Context, res *resource.Resource) error {
	incomingResource := fromResourceToModel(res)
	if err := r.db.WithContext(ctx).Create(incomingResource).Error; err != nil {
		return errors.Wrap(resource.EntityResource, "error creating resource to database", err)
	}
	return nil
}

func (r Repository) Update(ctx context.Context, res *resource.Resource) error {
	inRes := fromResourceToModel(res)
	return r.update(r.db.WithContext(ctx), inRes)
}

func (r Repository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error) {
	var namespaceName string
	if name, err := tnnt.NamespaceName(); err == nil {
		namespaceName = name.String()
	}
	res, err := r.readByFullName(r.db.WithContext(ctx), tnnt.ProjectName().String(), namespaceName, store.String(), fullName)
	if err != nil {
		return nil, err
	}
	return fromModelToResource(res)
}

func (r Repository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	namespaceName, err := tnnt.NamespaceName()
	if err != nil {
		return nil, err
	}
	var resources []*Resource
	if err := r.db.WithContext(ctx).
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

func (r Repository) UpdateAll(ctx context.Context, resources []*resource.Resource) error {
	resourceModels := make([]*Resource, len(resources))
	for i, res := range resources {
		resourceModels[i] = fromResourceToModel(res)
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, m := range resourceModels {
			if err := r.update(tx, m); err != nil {
				return errors.Wrap(resource.EntityResource, "error updating resource to database", err)
			}
		}
		return nil
	})
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

func (r Repository) UpdateStatus(ctx context.Context, store resource.Store, resources ...*resource.Resource) error {
	err := errors.NewMultiError("errors during resource status update")

	err.Append(r.updateColumn(ctx, store, resource.StatusUpdateFailure, resources...))
	err.Append(r.updateColumn(ctx, store, resource.StatusCreateFailure, resources...))
	err.Append(r.updateColumn(ctx, store, resource.StatusSuccess, resources...))
	return err
}

func (r Repository) updateColumn(ctx context.Context, store resource.Store, status resource.Status, resources ...*resource.Resource) error {
	var resourceNames []string

	for _, res := range resources {
		if res.Status() == status {
			resourceNames = append(resourceNames, res.FullName())
		}
	}
	if len(resourceNames) == 0 {
		return nil
	}

	err := r.db.WithContext(ctx).Model(Resource{}).Where("store = ?", store.String()).
		Where("full_name IN ?", resourceNames).
		UpdateColumn("status", status.String()).Error
	if err != nil {
		err = errors.Wrap(resource.EntityResource, "error updating resource to database", err)
	}
	return nil
}
