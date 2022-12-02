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
	FullName string
	Kind     string
	Store    string

	ProjectName   string
	NamespaceName string

	Metadata datatypes.JSON
	Spec     datatypes.JSON

	URN string

	Status string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func fromResourceToModel(r *resource.Resource) *Resource {
	metadata, _ := json.Marshal(r.Metadata())
	spec, _ := json.Marshal(r.Spec())
	return &Resource{
		FullName:      r.FullName(),
		Kind:          r.Kind().String(),
		Store:         r.Store().String(),
		ProjectName:   r.Tenant().ProjectName().String(),
		NamespaceName: r.Tenant().NamespaceName().String(),
		Metadata:      metadata,
		Spec:          spec,
		URN:           r.URN(),
		Status:        r.Status().String(),
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
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
