package resource

import (
	"encoding/json"
	"time"

	"gorm.io/datatypes"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type Resource struct {
	FullName string `gorm:"not null"`
	Kind     string `gorm:"not null"`
	Store    string `gorm:"not null"`

	ProjectName   string `gorm:"not null"`
	NamespaceName string `gorm:"not null"`

	Metadata datatypes.JSON `gorm:"not null"`
	Spec     datatypes.JSON `gorm:"not null"`

	URN string `gorm:"not null"`

	Status string `gorm:"not null"`

	CreatedAt time.Time `gorm:"not null"`
	UpdatedAt time.Time `gorm:"not null"`
}

func fromResourceToModel(r *resource.Resource) (*Resource, error) {
	var namespaceName string
	if name, err := r.Tenant().NamespaceName(); err == nil {
		namespaceName = name.String()
	}
	metadata, err := json.Marshal(r.Metadata())
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error marshalling metadata", err)
	}
	spec, err := json.Marshal(r.Spec())
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error marshalling spec", err)
	}
	return &Resource{
		FullName:      r.FullName(),
		Kind:          r.Kind().String(),
		Store:         r.Dataset().Store.String(),
		ProjectName:   r.Tenant().ProjectName().String(),
		NamespaceName: namespaceName,
		Metadata:      metadata,
		Spec:          spec,
		URN:           r.URN(),
		Status:        r.Status().String(),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

func fromModelToResource(r *Resource) (*resource.Resource, error) {
	kind, err := resource.FromStringToKind(r.Kind)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error constructing kind", err)
	}
	store, err := resource.FromStringToStore(r.Store)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error constructing kind", err)
	}
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityTenant, "error constructing new tenant", err)
	}
	var metadata *resource.Metadata
	if err := json.Unmarshal(r.Metadata, &metadata); err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error unmarshalling metadata", err)
	}
	var spec map[string]any
	if err := json.Unmarshal(r.Spec, &spec); err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error unmarshalling spec", err)
	}
	output, err := resource.NewResource(r.FullName, kind, store, tnnt, metadata, spec)
	if err == nil {
		output = resource.FromExisting(output, resource.ReplaceStatus(resource.FromStringToStatus(r.Status)))
	}
	return output, err
}
