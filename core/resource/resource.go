package resource

import (
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

	dataset Dataset
	kind    Kind

	tenant tenant.Tenant

	spec     map[string]any
	metadata *Metadata
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

func (r *Resource) Metadata() *Metadata {
	return r.metadata
}

func (r *Resource) Kind() Kind {
	return r.kind
}

func (r *Resource) Tenant() tenant.Tenant {
	return r.tenant
}

func (r *Resource) Spec() map[string]any {
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

	dataset, err := DataSetFrom(store, sections[0], sections[1])
	if err != nil {
		return nil, err
	}

	if meta == nil {
		return nil, errors.InvalidArgument(EntityResource, "invalid resource metadata")
	}

	return &Resource{
		name:     name,
		dataset:  dataset,
		kind:     kind,
		tenant:   tnnt,
		spec:     spec,
		metadata: meta,
	}, nil
}


func (r *Resource) Validate() error {
	switch r.kind {
	case KindTable:
		var table Table
		if err := mapstructure.Decode(r.spec, &table); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode table spec for "+r.FullName())
		}
		return table.Validate()

	case KindExternalTable:
		var externalTable ExternalTable
		if err := mapstructure.Decode(r.spec, &externalTable); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode external spec for "+r.FullName())
		}
		return externalTable.Validate()

	case KindView:
		var view View
		if err := mapstructure.Decode(r.spec, &view); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode view spec for "+r.FullName())
		}
		return view.Validate()

	case KindDataset:
		var dataset DatasetDetails
		if err := mapstructure.Decode(r.spec, &dataset); err != nil {
			return errors.InvalidArgument(EntityResource, "not able to decode dataset spec for "+r.FullName())
		}
		return dataset.Validate()

	default:
		return errors.InvalidArgument(EntityResource, "unknown kind")
	}
}
