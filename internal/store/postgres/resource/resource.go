package resource

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/internal/errors"
)

type Resource struct {
	ID uuid.UUID

	FullName string
	Kind     string
	Store    string

	ProjectName   string
	NamespaceName string

	Metadata json.RawMessage
	Spec     map[string]any

	URN string

	Status string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func FromResourceToModel(r *resource.Resource) *Resource {
	metadata, _ := json.Marshal(r.Metadata())

	return &Resource{
		FullName:      r.FullName(),
		Kind:          r.Kind(),
		Store:         r.Store().String(),
		ProjectName:   r.Tenant().ProjectName().String(),
		NamespaceName: r.Tenant().NamespaceName().String(),
		Metadata:      metadata,
		Spec:          r.Spec(),
		URN:           r.URN(),
		Status:        r.Status().String(),
	}
}

func FromModelToResource(r *Resource) (*resource.Resource, error) {
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

	output, err := resource.NewResource(r.FullName, r.Kind, store, tnnt, metadata, r.Spec)
	if err == nil {
		output = resource.FromExisting(output, resource.ReplaceStatus(resource.FromStringToStatus(r.Status)))
		output.UpdateURN(r.URN)
	}
	return output, err
}
