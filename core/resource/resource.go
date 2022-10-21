package resource

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityResource       = "resource"
	nameSectionSeparator = "."

	DatesetNameSections = 2
	TableNameSections   = 3
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

func NewResource(fullName string, kind Kind, store Store, tnnt tenant.Tenant, meta *Metadata, spec map[string]any) (*Resource, error) {
	sections := strings.Split(fullName, nameSectionSeparator)
	var strName string
	if kind == KindDataset {
		if len(sections) != DatesetNameSections {
			return nil, errors.InvalidArgument(EntityResource, "invalid dataset name: "+fullName)
		}
		strName = sections[1]
	} else {
		if len(sections) != TableNameSections {
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

func (r *Resource) Name() Name {
	return r.name
}

func (r *Resource) FullName() string {
	if r.kind == KindDataset {
		return r.dataset.FullName()
	}
	return r.dataset.FullName() + "." + r.name.String()
}

func (r *Resource) URN() string {
	if r.kind == KindDataset {
		return r.dataset.URN()
	}
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

func (r *Resource) Dataset() Dataset {
	return r.dataset
}

func (r *Resource) Status() Status {
	return r.status
}

func (r *Resource) Spec() map[string]any {
	return r.spec
}

func (r *Resource) Validate() error {
	switch r.kind {
	case KindTable:
		table, err := ConvertSpecTo[Table](r)
		if err != nil {
			return err
		}
		table.Name = r.name
		table.Dataset = r.dataset
		return table.Validate()

	case KindExternalTable:
		externalTable, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			return err
		}
		externalTable.Name = r.name
		externalTable.Dataset = r.dataset
		return externalTable.Validate()

	case KindView:
		view, err := ConvertSpecTo[View](r)
		if err != nil {
			return err
		}
		view.Name = r.name
		view.Dataset = r.dataset
		return view.Validate()

	case KindDataset:
		ds, err := ConvertSpecTo[DatasetDetails](r)
		if err != nil {
			return err
		}
		ds.Dataset = r.dataset
		return ds.Validate()

	default:
		return errors.InvalidArgument(EntityResource, "unknown kind")
	}
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
	if r.dataset != incoming.dataset {
		return false
	}
	if r.tenant != incoming.tenant {
		return false
	}
	if !reflect.DeepEqual(r.spec, incoming.spec) {
		return false
	}
	if !reflect.DeepEqual(r.metadata, incoming.metadata) {
		return false
	}
	return r.status == incoming.status
}

func (r *Resource) MarkSuccess() error {
	if r.status == StatusToCreate || r.status == StatusToUpdate {
		r.status = StatusSuccess
		return nil
	}
	msg := fmt.Sprintf("invalid transition from %s to %s for %s", r.status, StatusSuccess, r.FullName())
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkFailed() error {
	if r.status == StatusToCreate {
		r.status = StatusCreateFailure
		return nil
	} else if r.status == StatusToUpdate {
		r.status = StatusUpdateFailure
		return nil
	}
	msg := fmt.Sprintf("invalid transition from %s to failure for %s", r.status, r.FullName())
	return errors.InvalidStateTransition(EntityResource, msg)
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

func ConvertSpecTo[T DatasetDetails | Table | View | ExternalTable](res *Resource) (*T, error) {
	var spec T
	if err := mapstructure.Decode(res.spec, &spec); err != nil {
		msg := fmt.Sprintf("%s: not able to decode spec for %s", err, res.FullName())
		return nil, errors.InvalidArgument(EntityResource, msg)
	}

	return &spec, nil
}
