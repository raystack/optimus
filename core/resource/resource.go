package resource

import (
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityResource       = "resource"
	nameSectionSeparator = "."
)

type ValidateResource interface {
	Validate() error
}

type UniqueResource interface {
	URN() string
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

	kind    Kind
	dataset Dataset

	tenant tenant.Tenant

	spec     map[string]any
	metadata *Metadata

	status Status
}

func (r Resource) Name() Name {
	return r.name
}

func (r Resource) FullName() string {
	if r.kind == KindDataset {
		return r.dataset.FullName()
	}
	return r.dataset.FullName() + "." + r.name.String()
}

func (r Resource) URN() string {
	if r.kind == KindDataset {
		return r.dataset.URN()
	}
	return r.dataset.URN() + "." + r.name.String()
}

func (r Resource) Metadata() *Metadata {
	return r.metadata
}

func (r Resource) Kind() Kind {
	return r.kind
}

func (r Resource) Tenant() tenant.Tenant {
	return r.tenant
}

func (r Resource) Dataset() Dataset {
	return r.dataset
}

func (r Resource) Status() Status {
	return r.status
}

func (r Resource) Spec() map[string]any {
	return r.spec
}

func NewResource(fullName string, kind Kind, store Store, tnnt tenant.Tenant, meta *Metadata, spec map[string]any) (*Resource, error) {
	sections := strings.Split(fullName, nameSectionSeparator)
	var strName string
	if kind == KindDataset {
		if len(sections) != 2 {
			return nil, errors.InvalidArgument(EntityResource, "invalid dataset name: "+fullName)
		}
		strName = sections[1]
	} else {
		if len(sections) != 3 {
			return nil, errors.InvalidArgument(EntityResource, "invalid resource name: "+fullName)
		}
		strName = sections[2]
	}

	name, err := NameFrom(strName)
	if err != nil {
		return nil, err
	}

	if len(spec) == 0 {
		return nil, errors.InvalidArgument(EntityResource, "invalid resource spec for "+fullName)
	}

	dataset, err := DataSetFrom(store, sections[0], sections[1])
	if err != nil {
		return nil, err
	}

	if meta == nil {
		return nil, errors.InvalidArgument(EntityResource, "invalid resource metadata")
	}

	return &Resource{
		name:     name,
		kind:     kind,
		dataset:  dataset,
		tenant:   tnnt,
		spec:     spec,
		metadata: meta,
		status:   StatusUnknown,
	}, nil
}

func (r Resource) Validate() error {
	switch r.kind {
	case KindTable:
		var table Table
		if err := mapstructure.Decode(r.spec, &table); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode table spec for "+r.FullName())
		}
		table.Name = r.name
		table.Dataset = r.dataset
		return table.Validate()

	case KindExternalTable:
		var externalTable ExternalTable
		if err := mapstructure.Decode(r.spec, &externalTable); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode external spec for "+r.FullName())
		}
		externalTable.Name = r.name
		externalTable.Dataset = r.dataset
		return externalTable.Validate()

	case KindView:
		var view View
		if err := mapstructure.Decode(r.spec, &view); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode view spec for "+r.FullName())
		}
		view.Name = r.name
		view.Dataset = r.dataset
		return view.Validate()

	case KindDataset:
		var dataset DatasetDetails
		if err := mapstructure.Decode(r.spec, &dataset); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode dataset spec for "+r.FullName())
		}
		dataset.Dataset = r.dataset
		return dataset.Validate()

	default:
		return errors.InvalidArgument(EntityResource, "unknown kind")
	}
}

func (r Resource) Equal(incoming *Resource) bool {
	return reflect.DeepEqual(&r, incoming)
}

type FromExistingOpt func(r *Resource)

func ReplaceDataset(dataset Dataset) FromExistingOpt {
	return func(r *Resource) {
		r.dataset = dataset
	}
}

func ReplaceTenant(tnnt tenant.Tenant) FromExistingOpt {
	return func(r *Resource) {
		r.tenant = tnnt
	}
}

func ReplaceStatus(status Status) FromExistingOpt {
	return func(r *Resource) {
		r.status = status
	}
}

func FromExisting(existing *Resource, opts ...FromExistingOpt) *Resource {
	output := &Resource{
		name:     existing.name,
		kind:     existing.kind,
		dataset:  existing.dataset,
		tenant:   existing.tenant,
		spec:     existing.spec,
		metadata: existing.metadata,
		status:   existing.status,
	}
	for _, opt := range opts {
		opt(output)
	}
	return output
}
