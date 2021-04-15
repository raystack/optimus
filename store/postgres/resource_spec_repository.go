package postgres

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/datatypes"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

type Resource struct {
	ID        uuid.UUID `gorm:"primary_key;type:uuid"`
	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	Version   int
	Name      string `gorm:"not null"`
	Type      string `gorm:"not null"`
	Datastore string `gorm:"not null"`

	Spec   []byte
	Assets datatypes.JSON
	Labels datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt *time.Time
}

func (r Resource) FromSpec(resourceSpec models.ResourceSpec, proj models.ProjectSpec) (Resource, error) {

	// serialize resource spec without assets to one of the datastore provided wire format
	binaryReadySpec := resourceSpec
	binaryReadySpec.Assets = nil
	controller, ok := resourceSpec.Datastore.Types()[resourceSpec.Type]
	if !ok {
		return Resource{}, fmt.Errorf("unknown type of datastore %s", resourceSpec.Type)
	}
	serializedSpec, err := controller.Adapter().ToYaml(binaryReadySpec)
	if err != nil {
		return Resource{}, err
	}

	assetBytes, err := json.Marshal(resourceSpec.Assets)
	if err != nil {
		return Resource{}, err
	}
	labelBytes, err := json.Marshal(resourceSpec.Labels)
	if err != nil {
		return Resource{}, err
	}

	return Resource{
		ID:        resourceSpec.ID,
		ProjectID: proj.ID,
		Version:   resourceSpec.Version,
		Name:      resourceSpec.Name,
		Type:      resourceSpec.Type.String(),
		Datastore: resourceSpec.Datastore.Name(),
		Spec:      serializedSpec,
		Assets:    assetBytes,
		Labels:    labelBytes,
	}, nil
}

func (r Resource) ToSpec(ds models.Datastorer) (models.ResourceSpec, error) {
	resourceType := models.ResourceType(r.Type)

	// deserialize resource spec without assets to one of the datastore provided wire format
	controller, ok := ds.Types()[resourceType]
	if !ok {
		return models.ResourceSpec{}, fmt.Errorf("unknown type of datastore %s", resourceType)
	}
	deserializedSpec, err := controller.Adapter().FromYaml(r.Spec)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	var assets map[string]string
	if err := json.Unmarshal(r.Assets, &assets); err != nil {
		return models.ResourceSpec{}, err
	}
	var labels map[string]string
	if err := json.Unmarshal(r.Labels, &labels); err != nil {
		return models.ResourceSpec{}, err
	}

	return models.ResourceSpec{
		ID:        r.ID,
		Version:   r.Version,
		Name:      r.Name,
		Type:      resourceType,
		Datastore: ds,
		Spec:      deserializedSpec.Spec,
		Assets:    assets,
		Labels:    labels,
	}, nil
}

type resourceSpecRepository struct {
	db        *gorm.DB
	project   models.ProjectSpec
	datastore models.Datastorer
}

func (repo *resourceSpecRepository) Insert(resource models.ResourceSpec) error {
	if len(resource.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	p, err := Resource{}.FromSpec(resource, repo.project)
	if err != nil {
		return err
	}
	return repo.db.Create(&p).Error
}

func (repo *resourceSpecRepository) Save(spec models.ResourceSpec) error {
	existingResource, err := repo.GetByName(spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find resource by name")
	}
	resource, err := Resource{}.FromSpec(spec, repo.project)
	if err != nil {
		return err
	}
	if err == nil {
		resource.ID = existingResource.ID
	}
	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *resourceSpecRepository) GetByName(name string) (models.ResourceSpec, error) {
	var r Resource
	if err := repo.db.Where("project_id = ? AND datastore = ? AND name = ?", repo.project.ID, repo.datastore.Name(), name).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ResourceSpec{}, store.ErrResourceNotFound
		}
		return models.ResourceSpec{}, err
	}
	return r.ToSpec(repo.datastore)
}

func (repo *resourceSpecRepository) GetByID(id uuid.UUID) (models.ResourceSpec, error) {
	var r Resource
	if err := repo.db.Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ResourceSpec{}, store.ErrResourceNotFound
		}
		return models.ResourceSpec{}, err
	}
	return r.ToSpec(repo.datastore)
}

func (repo *resourceSpecRepository) GetAll() ([]models.ResourceSpec, error) {
	specs := []models.ResourceSpec{}
	resources := []Resource{}
	if err := repo.db.Where("project_id = ? AND datastore = ?", repo.project.ID, repo.datastore.Name()).Find(&resources).Error; err != nil {
		return specs, err
	}
	for _, r := range resources {
		adapted, err := r.ToSpec(repo.datastore)
		if err != nil {
			return specs, errors.Wrap(err, "failed to adapt resource")
		}
		specs = append(specs, adapted)
	}
	return specs, nil
}

func (repo *resourceSpecRepository) Delete(name string) error {
	return repo.db.Where("project_id = ? AND datastore = ? AND name = ? ", repo.project.ID, repo.datastore.Name(), name).Delete(&Resource{}).Error
}

func NewResourceSpecRepository(db *gorm.DB, project models.ProjectSpec, ds models.Datastorer) *resourceSpecRepository {
	return &resourceSpecRepository{
		db:        db,
		project:   project,
		datastore: ds,
	}
}
