package mock

import (
	"context"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/core/progress"

	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/mock"
)

type Datastorer struct {
	mock.Mock
}

func (d *Datastorer) Name() string {
	return d.Called().Get(0).(string)
}
func (d *Datastorer) Description() string {
	return d.Called().Get(0).(string)
}
func (d *Datastorer) Types() map[models.ResourceType]models.DatastoreTypeController {
	return d.Called().Get(0).(map[models.ResourceType]models.DatastoreTypeController)
}
func (d *Datastorer) CreateResource(ctx context.Context, inp models.CreateResourceRequest) error {
	return d.Called(ctx, inp).Error(0)
}
func (d *Datastorer) UpdateResource(ctx context.Context, inp models.UpdateResourceRequest) error {
	return d.Called(ctx, inp).Error(0)
}
func (d *Datastorer) ReadResource(ctx context.Context, inp models.ReadResourceRequest) (models.ReadResourceResponse, error) {
	args := d.Called(ctx, inp)
	return args.Get(0).(models.ReadResourceResponse), args.Error(1)
}
func (d *Datastorer) DeleteResource(ctx context.Context, inp models.DeleteResourceRequest) error {
	return d.Called(ctx, inp).Error(0)
}

type DatastoreTypeController struct {
	mock.Mock
}

func (d *DatastoreTypeController) Adapter() models.DatastoreSpecAdapter {
	return d.Called().Get(0).(models.DatastoreSpecAdapter)
}

func (d *DatastoreTypeController) Validator() models.DatastoreSpecValidator {
	return d.Called().Get(0).(models.DatastoreSpecValidator)
}

func (d *DatastoreTypeController) DefaultAssets() map[string]string {
	return d.Called().Get(0).(map[string]string)
}

type DatastoreTypeAdapter struct {
	mock.Mock
}

func (d *DatastoreTypeAdapter) ToYaml(spec models.ResourceSpec) ([]byte, error) {
	args := d.Called(spec)
	return args.Get(0).([]byte), args.Error(1)
}

func (d *DatastoreTypeAdapter) FromYaml(bytes []byte) (models.ResourceSpec, error) {
	args := d.Called(bytes)
	return args.Get(0).(models.ResourceSpec), args.Error(1)
}

func (d *DatastoreTypeAdapter) ToProtobuf(spec models.ResourceSpec) ([]byte, error) {
	args := d.Called(spec)
	return args.Get(0).([]byte), args.Error(1)
}

func (d *DatastoreTypeAdapter) FromProtobuf(bytes []byte) (models.ResourceSpec, error) {
	args := d.Called(bytes)
	return args.Get(0).(models.ResourceSpec), args.Error(1)
}

type DatastoreService struct {
	mock.Mock
}

func (d *DatastoreService) GetAll(spec models.NamespaceSpec, datastoreName string) ([]models.ResourceSpec, error) {
	args := d.Called(spec, datastoreName)
	return args.Get(0).([]models.ResourceSpec), args.Error(1)
}

func (d *DatastoreService) CreateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	return d.Called(ctx, namespace, resourceSpecs, obs).Error(0)
}

func (d *DatastoreService) UpdateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	return d.Called(ctx, namespace, resourceSpecs, obs).Error(0)
}

func (d *DatastoreService) ReadResource(ctx context.Context, namespace models.NamespaceSpec, datastoreName, name string) (models.ResourceSpec, error) {
	args := d.Called(ctx, namespace, datastoreName, name)
	return args.Get(0).(models.ResourceSpec), args.Error(1)
}

func (d *DatastoreService) DeleteResource(ctx context.Context, namespace models.NamespaceSpec, datastoreName, name string) error {
	return d.Called(ctx, namespace, datastoreName, name).Error(1)
}

type SupportedDatastoreRepo struct {
	mock.Mock
}

func (repo *SupportedDatastoreRepo) GetByName(name string) (models.Datastorer, error) {
	args := repo.Called(name)
	return args.Get(0).(models.Datastorer), args.Error(1)
}

func (repo *SupportedDatastoreRepo) GetAll() []models.Datastorer {
	args := repo.Called()
	return args.Get(0).([]models.Datastorer)
}

func (repo *SupportedDatastoreRepo) Add(t models.Datastorer) error {
	return repo.Called(t).Error(0)
}

type ResourceSpecRepoFactory struct {
	mock.Mock
}

func (r *ResourceSpecRepoFactory) New(spec models.NamespaceSpec, storer models.Datastorer) store.ResourceSpecRepository {
	return r.Called(spec, storer).Get(0).(store.ResourceSpecRepository)
}

type ResourceSpecRepository struct {
	mock.Mock
}

func (r *ResourceSpecRepository) Save(spec models.ResourceSpec) error {
	return r.Called(spec).Error(0)
}

func (r *ResourceSpecRepository) GetByName(s string) (models.ResourceSpec, error) {
	args := r.Called(s)
	return args.Get(0).(models.ResourceSpec), args.Error(1)
}

func (r *ResourceSpecRepository) GetAll() ([]models.ResourceSpec, error) {
	args := r.Called()
	return args.Get(0).([]models.ResourceSpec), args.Error(1)
}

func (r *ResourceSpecRepository) Delete(s string) error {
	return r.Called(s).Error(0)
}

type ProjectResourceSpecRepoFactory struct {
	mock.Mock
}

func (r *ProjectResourceSpecRepoFactory) New(spec models.ProjectSpec, storer models.Datastorer) store.ProjectResourceSpecRepository {
	return r.Called(spec, storer).Get(0).(store.ProjectResourceSpecRepository)
}

type ProjectResourceSpecRepository struct {
	mock.Mock
}

func (r *ProjectResourceSpecRepository) GetByName(s string) (models.ResourceSpec, error) {
	args := r.Called(s)
	return args.Get(0).(models.ResourceSpec), args.Error(1)
}

func (r *ProjectResourceSpecRepository) GetAll() ([]models.ResourceSpec, error) {
	args := r.Called()
	return args.Get(0).([]models.ResourceSpec), args.Error(1)
}
