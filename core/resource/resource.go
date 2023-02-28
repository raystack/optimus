package resource

import (
	"reflect"
	"strings"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityResource       = "resource"
	nameSectionSeparator = "."
)

type Metadata struct {
	Version     int32
	Description string
	Labels      map[string]string
}

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

	kind  string
	store Store
	urn   string

	tenant tenant.Tenant

	spec     map[string]any
	metadata *Metadata

	status Status
}

func NewResource(fullName string, kind string, store Store, tnnt tenant.Tenant, meta *Metadata, spec map[string]any) (*Resource, error) {
	name, err := NameFrom(fullName)
	if err != nil {
		return nil, err
	}

	if len(spec) == 0 {
		return nil, errors.InvalidArgument(EntityResource, "empty resource spec for "+fullName)
	}

	if meta == nil {
		return nil, errors.InvalidArgument(EntityResource, "empty resource metadata for "+fullName)
	}

	return &Resource{
		name:     name,
		kind:     kind,
		store:    store,
		tenant:   tnnt,
		spec:     spec,
		metadata: meta,
		status:   StatusUnknown,
	}, nil
}

func (r *Resource) Name() Name {
	return r.name
}

func (r *Resource) FullName() string {
	return r.name.String()
}

func (r *Resource) URN() string {
	return r.urn
}

func (r *Resource) UpdateURN(urn string) error {
	if r.urn == "" {
		r.urn = urn
		return nil
	}

	return errors.InvalidArgument(EntityResource, "urn already present for "+r.FullName())
}

func (r *Resource) Metadata() *Metadata {
	return r.metadata
}

func (r *Resource) NameSections() []string {
	return strings.Split(r.name.String(), nameSectionSeparator)
}

func (r *Resource) Kind() string {
	return r.kind
}

func (r *Resource) Tenant() tenant.Tenant {
	return r.tenant
}

func (r *Resource) Store() Store {
	return r.store
}

func (r *Resource) Status() Status {
	return r.status
}

func (r *Resource) Spec() map[string]any {
	return r.spec
}

func (r *Resource) Equal(incoming *Resource) bool {
	if r == nil || incoming == nil {
		return r == nil && incoming == nil
	}
	if r.name != incoming.name {
		return false
	}
	if r.kind != incoming.kind {
		return false
	}
	if r.store != incoming.store {
		return false
	}
	if !reflect.DeepEqual(r.tenant, incoming.tenant) {
		return false
	}
	if !reflect.DeepEqual(r.spec, incoming.spec) {
		return false
	}
	return reflect.DeepEqual(r.metadata, incoming.metadata)
}

type FromExistingOpt func(r *Resource)

func ReplaceStatus(status Status) FromExistingOpt {
	return func(r *Resource) {
		r.status = status
	}
}

func FromExisting(existing *Resource, opts ...FromExistingOpt) *Resource {
	output := &Resource{
		name:     existing.name,
		kind:     existing.kind,
		store:    existing.store,
		tenant:   existing.tenant,
		spec:     existing.spec,
		metadata: existing.metadata,
		urn:      existing.urn,
		status:   existing.status,
	}
	for _, opt := range opts {
		opt(output)
	}
	return output
}
