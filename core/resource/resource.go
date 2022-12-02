package resource

import (
	"fmt"
	"reflect"
	"strings"

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

	kind  Kind
	store Store

	tenant tenant.Tenant

	spec     map[string]any
	metadata *Metadata

	status Status
}

func NewResource(fullName string, kind Kind, store Store, tnnt tenant.Tenant, meta *Metadata, spec map[string]any) (*Resource, error) {
	name, err := NameFrom(fullName)
	if err != nil {
		return nil, err
	}

	if len(spec) == 0 {
		return nil, errors.InvalidArgument(EntityResource, "invalid resource spec for "+fullName)
	}

	if meta == nil {
		return nil, errors.InvalidArgument(EntityResource, "invalid resource metadata")
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

// TODO: this is bad, URN now should come from the store
func (r *Resource) URN() (string, error) {
	sections := strings.Split(r.name.String(), nameSectionSeparator)
	if r.kind == KindDataset {
		if len(sections) != DatesetNameSections {
			return "", errors.InvalidArgument(EntityResource, "invalid dataset name: "+r.name.String())
		}
	} else {
		if len(sections) != TableNameSections {
			return "", errors.InvalidArgument(EntityResource, "invalid resource name: "+r.name.String())
		}
	}

	datasetURN := string(r.store) + "://" + sections[0] + ":" + sections[1]
	if r.kind == KindDataset {
		return datasetURN, nil
	}
	return datasetURN + "." + sections[2], nil
}

func (r *Resource) Metadata() *Metadata {
	return r.metadata
}

func (r *Resource) NameSections() []string {
	return strings.Split(r.name.String(), nameSectionSeparator)
}

func (r *Resource) Kind() Kind {
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

func (r *Resource) MarkValidationSuccess() error {
	if r.status == StatusUnknown {
		r.status = StatusValidationSuccess
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusValidationSuccess)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkValidationFailure() error {
	if r.status == StatusUnknown {
		r.status = StatusValidationFailure
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusValidationFailure)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkSkipped() error {
	if r.status == StatusValidationSuccess {
		r.status = StatusSkipped
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusSkipped)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkToCreate() error {
	if r.status == StatusValidationSuccess {
		r.status = StatusToCreate
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusToCreate)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkToUpdate() error {
	if r.status == StatusValidationSuccess {
		r.status = StatusToUpdate
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusToUpdate)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkExistInStore() error {
	if r.status == StatusToCreate {
		r.status = StatusExistInStore
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status [%s] is not allowed", r.FullName(), r.status, StatusExistInStore)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkFailure() error {
	switch r.status {
	case StatusToCreate:
		r.status = StatusCreateFailure
		return nil
	case StatusToUpdate:
		r.status = StatusUpdateFailure
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status failure is not allowed", r.FullName(), r.status)
	return errors.InvalidStateTransition(EntityResource, msg)
}

func (r *Resource) MarkSuccess() error {
	switch r.status {
	case StatusToCreate, StatusToUpdate:
		r.status = StatusSuccess
		return nil
	}
	msg := fmt.Sprintf("status transition for [%s] from status [%s] to status success is not allowed", r.FullName(), r.status)
	return errors.InvalidStateTransition(EntityResource, msg)
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
		status:   existing.status,
	}
	for _, opt := range opts {
		opt(output)
	}
	return output
}
