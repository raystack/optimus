package resource

import (
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityResource = "resource"
	separator      = "."
)

type Name string

func NameFrom(name string) (Name, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityResource, "resource name is empty")
	}

	return Name(name), nil
}

func (n Name) String() string {
	return string(n)
}

type Resource struct {
	name Name

	dataset Dataset
	kind    Kind

	tenant tenant.Tenant

	spec string
}

func (r *Resource) Name() Name {
	return r.name
}

func (r *Resource) FullName() string {
	return r.dataset.FullName() + "." + r.name.String()
}

func (r *Resource) URN() string {
	return r.dataset.URN() + "." + r.name.String()
}

func NewResource(fullName string, kind Kind, tnnt tenant.Tenant, spec string) (*Resource, error) {

	return nil, nil
}

type WithMetadata struct {
	resource *Resource

	metadata *Metadata
}

func NewResourceWithMetadata() (*WithMetadata, error) {
	return nil, nil
}
