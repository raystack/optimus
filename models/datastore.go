package models

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/progress"
)

const (
	ResourceTypeTable         ResourceType = "table"
	ResourceTypeDataset       ResourceType = "dataset"
	ResourceTypeView          ResourceType = "view"
	ResourceTypeExternalTable ResourceType = "external_table"
)

type ResourceType string

func (r ResourceType) String() string {
	return string(r)
}

type ResourceSpec struct {
	ID        uuid.UUID
	Version   int
	Name      string
	Type      ResourceType
	Datastore Datastorer
	URN       string

	Spec   interface{}
	Assets ResourceAssets
	Labels map[string]string
}

type ResourceAssets map[string]string

func (r ResourceAssets) GetByName(n string) (string, bool) {
	v, ok := r[n]
	return v, ok
}

// Datastorer needs to be satisfied with supported data store types
// Datastore CRUD should be safe from race conditions
type Datastorer interface {
	Name() string
	Description() string
	Types() map[ResourceType]DatastoreTypeController

	// CreateResource will create the requested resource if not exists
	// if already exists, do nothing
	CreateResource(context.Context, CreateResourceRequest) error

	// UpdateResource will create the requested resource if not exists
	// if already exists, update it
	UpdateResource(context.Context, UpdateResourceRequest) error

	// ReadResource will read the requested resource if exists else error
	ReadResource(context.Context, ReadResourceRequest) (ReadResourceResponse, error)

	// DeleteResource will delete the requested resource if exists
	DeleteResource(context.Context, DeleteResourceRequest) error

	// BackupResource will backup the requested resource if exists
	BackupResource(context.Context, BackupResourceRequest) (BackupResourceResponse, error)
}

type DatastoreTypeController interface {
	Adapter() DatastoreSpecAdapter
	Validator() DatastoreSpecValidator
	GenerateURN(interface{}) (string, error)

	// assets that will be created as templates when the resource is created
	// for the first time
	DefaultAssets() map[string]string
}

// DatastoreSpecAdapter dictates how spec will be serialized/deserialized in
// various wire formats if needed
type DatastoreSpecAdapter interface {
	ToYaml(spec ResourceSpec) ([]byte, error)
	FromYaml([]byte) (ResourceSpec, error)
	ToProtobuf(ResourceSpec) ([]byte, error)
	FromProtobuf([]byte) (ResourceSpec, error)
}

// DatastoreSpecValidator verifies if resource is as expected, in case of validation
// failure, return with non nil error
type DatastoreSpecValidator func(spec ResourceSpec) error

type CreateResourceRequest struct {
	Resource ResourceSpec
	Project  ProjectSpec
}

type UpdateResourceRequest struct {
	Resource ResourceSpec
	Project  ProjectSpec
}

type ResourceExistsRequest struct {
	Resource ResourceSpec
	Project  ProjectSpec
}

type ReadResourceRequest struct {
	Resource ResourceSpec
	Project  ProjectSpec
}

type ReadResourceResponse struct {
	Resource ResourceSpec
}

type DeleteResourceRequest struct {
	Resource ResourceSpec
	Project  ProjectSpec
}

var (
	DatastoreRegistry = &supportedDatastore{
		data: map[string]Datastorer{},
	}
	ErrUnsupportedDatastore = errors.New("unsupported datastore requested")
	ErrUnsupportedResource  = errors.New("unsupported resource")
)

type DatastoreRepo interface {
	GetByName(string) (Datastorer, error)
	GetAll() []Datastorer
	Add(Datastorer) error
}

type supportedDatastore struct {
	data map[string]Datastorer
}

func (s *supportedDatastore) GetByName(name string) (Datastorer, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedDatastore)
}

func (s *supportedDatastore) GetAll() []Datastorer {
	list := []Datastorer{}
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *supportedDatastore) GetDestination() []Datastorer {
	list := []Datastorer{}
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *supportedDatastore) Add(newUnit Datastorer) error {
	if newUnit.Name() == "" {
		return fmt.Errorf("datastore name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[newUnit.Name()]; ok {
		return fmt.Errorf("datastore name already in use %s", newUnit.Name())
	}

	s.data[newUnit.Name()] = newUnit
	return nil
}

type DatastoreService interface {
	// does not really fetch resource metadata, just the user provided spec
	GetAll(ctx context.Context, namespace NamespaceSpec, datastoreName string) ([]ResourceSpec, error)
	CreateResource(ctx context.Context, namespace NamespaceSpec, resourceSpecs []ResourceSpec, obs progress.Observer) error
	UpdateResource(ctx context.Context, namespace NamespaceSpec, resourceSpecs []ResourceSpec, obs progress.Observer) error
	ReadResource(ctx context.Context, namespace NamespaceSpec, datastoreName, name string) (ResourceSpec, error)
	BackupResourceDryRun(ctx context.Context, backupRequest BackupRequest, jobSpecs []JobSpec) (BackupPlan, error)
	BackupResource(ctx context.Context, backupRequest BackupRequest, jobSpecs []JobSpec) (BackupResult, error)
	ListResourceBackups(ctx context.Context, projectSpec ProjectSpec, datastoreName string) ([]BackupSpec, error)
	GetResourceBackup(ctx context.Context, projectSpec ProjectSpec, datastoreName string, id uuid.UUID) (BackupSpec, error)
}
