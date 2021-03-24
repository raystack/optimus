package postgres

import (
	"encoding/json"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
	"github.com/odpf/optimus/models"
)

type Project struct {
	ID     uuid.UUID `gorm:"primary_key;type:uuid"`
	Name   string    `gorm:"not null;unique"`
	Config datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt *time.Time
}

func (p Project) FromSpec(spec models.ProjectSpec) (Project, error) {
	jsonBytes, err := json.Marshal(spec.Config)
	if err != nil {
		return Project{}, nil
	}
	return Project{
		ID:     spec.ID,
		Name:   spec.Name,
		Config: jsonBytes,
	}, nil
}

func (p Project) ToSpec() (models.ProjectSpec, error) {
	var conf map[string]string
	if err := json.Unmarshal(p.Config, &conf); err != nil {
		return models.ProjectSpec{}, nil
	}
	return models.ProjectSpec{
		ID:     p.ID,
		Name:   p.Name,
		Config: conf,
	}, nil
}

func (p Project) ToSpecWithSecrets(secrets []models.ProjectSecretItem) (models.ProjectSpec, error) {
	var conf map[string]string
	if err := json.Unmarshal(p.Config, &conf); err != nil {
		return models.ProjectSpec{}, nil
	}
	return models.ProjectSpec{
		ID:     p.ID,
		Name:   p.Name,
		Config: conf,
		Secret: secrets,
	}, nil
}

type projectRepository struct {
	db   *gorm.DB
	hash models.ApplicationKey
}

func (repo *projectRepository) Insert(resource models.ProjectSpec) error {
	p, err := Project{}.FromSpec(resource)
	if err != nil {
		return err
	}
	if len(p.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.Create(&p).Error
}

func (repo *projectRepository) Save(spec models.ProjectSpec) error {
	existingResource, err := repo.GetByName(spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find project by name")
	}
	resource, err := Project{}.FromSpec(spec)
	if err != nil {
		return err
	}
	if err == nil {
		resource.ID = existingResource.ID
	}
	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *projectRepository) GetByName(name string) (models.ProjectSpec, error) {
	var r Project
	if err := repo.db.Where("name = ?", name).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSpec{}, store.ErrResourceNotFound
		}
		return models.ProjectSpec{}, err
	}
	secrets, err := repo.fetchSecrets(r.ID)
	if err != nil {
		return models.ProjectSpec{}, errors.Wrap(err, "failed to fetch secrets")
	}
	return r.ToSpecWithSecrets(secrets)
}

func (repo *projectRepository) GetByID(id uuid.UUID) (models.ProjectSpec, error) {
	var r Project
	if err := repo.db.Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSpec{}, store.ErrResourceNotFound
		}
		return models.ProjectSpec{}, err
	}

	secrets, err := repo.fetchSecrets(r.ID)
	if err != nil {
		return models.ProjectSpec{}, errors.Wrap(err, "failed to fetch secrets")
	}
	return r.ToSpecWithSecrets(secrets)
}

func (repo *projectRepository) GetAll() ([]models.ProjectSpec, error) {
	specs := []models.ProjectSpec{}
	projs := []Project{}
	if err := repo.db.Find(&projs).Error; err != nil {
		return specs, err
	}
	for _, proj := range projs {
		secrets, err := repo.fetchSecrets(proj.ID)
		if err != nil {
			return specs, errors.Wrap(err, "failed to fetch secrets")
		}

		adapt, err := proj.ToSpecWithSecrets(secrets)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func (repo *projectRepository) fetchSecrets(projectID uuid.UUID) ([]models.ProjectSecretItem, error) {
	var r []Secret
	var adapted []models.ProjectSecretItem
	if err := repo.db.Where("project_id = ?", projectID).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// do nothing
		}
		return adapted, err
	}
	for _, secret := range r {
		secretItem, err := secret.ToSpec(repo.hash)
		if err != nil {
			return adapted, nil
		}
		adapted = append(adapted, secretItem)
	}
	return adapted, nil
}

func NewProjectRepository(db *gorm.DB, hash models.ApplicationKey) *projectRepository {
	return &projectRepository{
		db:   db,
		hash: hash,
	}
}
