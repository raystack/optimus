package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Project struct {
	ID     uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name   string    `gorm:"not null;unique"`
	Config datatypes.JSON

	// Secrets are read only and will not be saved by updating it here
	Secrets []Secret

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
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

func (p Project) ToSpecWithSecrets(h models.ApplicationKey) (models.ProjectSpec, error) {
	var conf map[string]string
	if err := json.Unmarshal(p.Config, &conf); err != nil {
		return models.ProjectSpec{}, nil
	}
	specSecrets := models.ProjectSecrets{}
	for _, sec := range p.Secrets {
		specSecret, err := sec.ToSpec(h)
		if err != nil {
			return models.ProjectSpec{}, err
		}
		specSecrets = append(specSecrets, specSecret)
	}
	return models.ProjectSpec{
		ID:     p.ID,
		Name:   p.Name,
		Config: conf,
		Secret: specSecrets,
	}, nil
}

type ProjectRepository struct {
	db   *gorm.DB
	hash models.ApplicationKey
}

func (repo *ProjectRepository) Insert(ctx context.Context, resource models.ProjectSpec) error {
	p, err := Project{}.FromSpec(resource)
	if err != nil {
		return err
	}
	if len(p.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.WithContext(ctx).Create(&p).Error
}

func (repo *ProjectRepository) Save(ctx context.Context, spec models.ProjectSpec) error {
	existingResource, err := repo.GetByName(ctx, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find project by name")
	}
	project, err := Project{}.FromSpec(spec)
	if err != nil {
		return err
	}
	project.ID = existingResource.ID
	return repo.db.WithContext(ctx).Omit("Secrets").Model(&project).Update("Config", project.Config).Error
}

func (repo *ProjectRepository) GetByName(ctx context.Context, name string) (models.ProjectSpec, error) {
	var r Project
	if err := repo.db.WithContext(ctx).Preload("Secrets").Where("name = ?", name).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSpec{}, store.ErrResourceNotFound
		}
		return models.ProjectSpec{}, err
	}
	return r.ToSpecWithSecrets(repo.hash)
}

func (repo *ProjectRepository) GetByID(ctx context.Context, id uuid.UUID) (models.ProjectSpec, error) {
	var r Project
	if err := repo.db.WithContext(ctx).Preload("Secrets").Where("id = ?", id).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSpec{}, store.ErrResourceNotFound
		}
		return models.ProjectSpec{}, err
	}
	return r.ToSpecWithSecrets(repo.hash)
}

func (repo *ProjectRepository) GetAll(ctx context.Context) ([]models.ProjectSpec, error) {
	var specs []models.ProjectSpec
	var projs []Project
	if err := repo.db.WithContext(ctx).Preload("Secrets").Find(&projs).Error; err != nil {
		return specs, err
	}
	for _, proj := range projs {
		adapt, err := proj.ToSpecWithSecrets(repo.hash)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func NewProjectRepository(db *gorm.DB, hash models.ApplicationKey) *ProjectRepository {
	return &ProjectRepository{
		db:   db,
		hash: hash,
	}
}
