package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Namespace struct {
	ID     uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name   string    `gorm:"not null;unique"`
	Config datatypes.JSON

	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func (Namespace) FromSpec(spec models.NamespaceSpec) Namespace {
	jsonBytes, err := json.Marshal(spec.Config)
	if err != nil {
		return Namespace{}
	}

	return Namespace{
		ID:     spec.ID,
		Name:   spec.Name,
		Config: jsonBytes,
	}
}

func (p Namespace) FromSpecWithProject(spec models.NamespaceSpec, proj models.ProjectSpec) Namespace {
	adaptNamespace := p.FromSpec(spec)

	adaptProject := Project{}.FromSpec(proj)

	adaptNamespace.Project = adaptProject
	adaptNamespace.ProjectID = adaptProject.ID

	return adaptNamespace
}

func (p Namespace) ToSpec(project models.ProjectSpec) (models.NamespaceSpec, error) {
	var conf map[string]string
	if err := json.Unmarshal(p.Config, &conf); err != nil {
		return models.NamespaceSpec{}, err
	}

	return models.NamespaceSpec{
		ID:          p.ID,
		Name:        p.Name,
		Config:      conf,
		ProjectSpec: project,
	}, nil
}

func (p Namespace) ToSpecWithProjectSecrets(hash models.ApplicationKey) (models.NamespaceSpec, error) {
	var conf map[string]string
	if err := json.Unmarshal(p.Config, &conf); err != nil {
		return models.NamespaceSpec{}, err
	}

	pSpec, err := p.Project.ToSpecWithSecrets(hash)
	if err != nil {
		return models.NamespaceSpec{}, err
	}
	return models.NamespaceSpec{
		ID:          p.ID,
		Name:        p.Name,
		Config:      conf,
		ProjectSpec: pSpec,
	}, nil
}

type namespaceRepository struct {
	db   *gorm.DB
	hash models.ApplicationKey
}

func (repo *namespaceRepository) Insert(ctx context.Context, project models.ProjectSpec, resource models.NamespaceSpec) error {
	c := Namespace{}.FromSpecWithProject(resource, project)

	if len(c.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.WithContext(ctx).Create(&c).Error
}

func (repo *namespaceRepository) Save(ctx context.Context, project models.ProjectSpec, spec models.NamespaceSpec) error {
	existingResource, err := repo.GetByName(ctx, project, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, project, spec)
	} else if err != nil {
		return fmt.Errorf("unable to find namespace by name: %w", err)
	}
	if len(spec.Config) == 0 {
		return store.ErrEmptyConfig
	}
	resource := Namespace{}.FromSpec(spec)

	resource.ID = existingResource.ID
	return repo.db.WithContext(ctx).Model(resource).Updates(resource).Error
}

func (repo *namespaceRepository) GetByName(ctx context.Context, project models.ProjectSpec, name string) (models.NamespaceSpec, error) {
	var r Namespace
	if err := repo.db.WithContext(ctx).Preload("Project").Preload("Project.Secrets").
		Where("name = ? AND project_id = ?", name, project.ID.UUID()).
		First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.NamespaceSpec{}, err
	}
	return r.ToSpecWithProjectSecrets(repo.hash)
}

func (repo *namespaceRepository) Get(ctx context.Context, projectName, namespaceName string) (models.NamespaceSpec, error) {
	var r Namespace
	err := repo.db.WithContext(ctx).
		Preload("Project").
		Preload("Project.Secrets").
		Joins("join project on namespace.project_id = project.id").
		Where("namespace.name = ? AND project.name = ?", namespaceName, projectName).
		First(&r).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.NamespaceSpec{}, err
	}
	return r.ToSpecWithProjectSecrets(repo.hash)
}

func (repo *namespaceRepository) GetAll(ctx context.Context, project models.ProjectSpec) ([]models.NamespaceSpec, error) {
	var specs []models.NamespaceSpec
	var namespaces []Namespace
	err := repo.db.WithContext(ctx).Preload("Project").Preload("Project.Secrets").
		Where("project_id = ?", project.ID.UUID()).
		Find(&namespaces).Error
	if err != nil {
		return specs, err
	}

	for _, namespace := range namespaces {
		adapt, err := namespace.ToSpecWithProjectSecrets(repo.hash)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func NewNamespaceRepository(db *gorm.DB, hash models.ApplicationKey) *namespaceRepository {
	return &namespaceRepository{
		db:   db,
		hash: hash,
	}
}
