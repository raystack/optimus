package resource

import (
	"context"
	"fmt"

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
	return r.create(r.db.WithContext(ctx), incomingResource)
}

func (r Repository) Update(ctx context.Context, res *resource.Resource) error {
	incomingResource := fromResourceToModel(res)
	return r.update(r.db.WithContext(ctx), incomingResource)
}

func (r Repository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string) (*resource.Resource, error) {
	res, err := r.readByFullName(r.db.WithContext(ctx), tnnt.ProjectName().String(), tnnt.NamespaceName().String(), store.String(), fullName)
	if err != nil {
		return nil, err
	}
	return fromModelToResource(res)
}

func (r Repository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	var resources []*Resource
	if err := r.db.WithContext(ctx).
		Where("project_name = ? and namespace_name = ? and store = ?",
			tnnt.ProjectName().String(), tnnt.NamespaceName().String(), store.String(),
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

func (r Repository) GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error) {
	var resources []*Resource
	result := r.db.WithContext(ctx).
		Where("project_name = ?", tnnt.ProjectName().String()).
		Where("namespace_name = ?", tnnt.NamespaceName().String()).
		Where("store = ?", store.String()).Where("full_name IN ?", names).
		Find(&resources)
	if result.Error != nil {
		return nil, errors.Wrap(resource.EntityResource, "error reading from database", result.Error)
	}

	var tenantResources = make([]*resource.Resource, len(resources))
	for i, res := range resources {
		model, err := fromModelToResource(res)
		if err != nil {
			return nil, err
		}
		tenantResources[i] = model
	}
	return tenantResources, nil
}

func (r Repository) UpdateStatus(ctx context.Context, resources ...*resource.Resource) error {
	resourceModels := make([]*Resource, len(resources))
	for i, res := range resources {
		resourceModels[i] = fromResourceToModel(res)
	}

	multiErr := errors.NewMultiError("error updating resources status")
	for _, m := range resourceModels {
		result := r.db.WithContext(ctx).Model(&Resource{}).
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
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(resource.EntityResource, fmt.Sprintf("resource [%s] is not found", fullName))
		}
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
