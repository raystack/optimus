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
	ID        uuid.UUID `gorm:"primary_key;type:uuid"`
	Name      string    `gorm:"not null;unique"`
	Config    datatypes.JSON
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
		Config: datatypes.JSON(jsonBytes),
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

type projectRepository struct {
	db *gorm.DB
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
	return r.ToSpec()
}

func (repo *projectRepository) GetByID(id uuid.UUID) (models.ProjectSpec, error) {
	var r Project
	if err := repo.db.Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSpec{}, store.ErrResourceNotFound
		}
		return models.ProjectSpec{}, err
	}
	return r.ToSpec()
}

func (repo *projectRepository) GetAll() ([]models.ProjectSpec, error) {
	specs := []models.ProjectSpec{}
	projs := []Project{}
	if err := repo.db.Find(&projs).Error; err != nil {
		return specs, err
	}
	for _, proj := range projs {
		adapt, err := proj.ToSpec()
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func NewProjectRepository(db *gorm.DB) *projectRepository {
	return &projectRepository{
		db: db,
	}
}
