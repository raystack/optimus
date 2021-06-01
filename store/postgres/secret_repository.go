package postgres

import (
	"encoding/base64"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/gtank/cryptopasta"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

type Secret struct {
	ID        uuid.UUID `gorm:"primary_key;type:uuid"`
	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	Name  string `gorm:"not null"`
	Value string

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt *time.Time
}

func (p Secret) FromSpec(spec models.ProjectSecretItem, proj models.ProjectSpec, hash models.ApplicationKey) (Secret, error) {
	// encrypt secret
	cipher, err := cryptopasta.Encrypt([]byte(spec.Value), hash.GetKey())
	if err != nil {
		return Secret{}, err
	}

	// base64 for storing safely in db
	base64cipher := base64.StdEncoding.EncodeToString(cipher)

	return Secret{
		ID:        spec.ID,
		Name:      spec.Name,
		Value:     base64cipher,
		ProjectID: proj.ID,
	}, nil
}

func (p Secret) ToSpec(hash models.ApplicationKey) (models.ProjectSecretItem, error) {
	// decode base64
	encrypted, err := base64.StdEncoding.DecodeString(p.Value)
	if err != nil {
		return models.ProjectSecretItem{}, err
	}

	// decrypt secret
	cleartext, err := cryptopasta.Decrypt(encrypted, hash.GetKey())
	if err != nil {
		return models.ProjectSecretItem{}, err
	}

	return models.ProjectSecretItem{
		ID:    p.ID,
		Name:  p.Name,
		Value: string(cleartext),
	}, nil
}

type secretRepository struct {
	db      *gorm.DB
	project models.ProjectSpec

	hash models.ApplicationKey
}

func (repo *secretRepository) Insert(resource models.ProjectSecretItem) error {
	p, err := Secret{}.FromSpec(resource, repo.project, repo.hash)
	if err != nil {
		return err
	}
	if len(p.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.Create(&p).Error
}

func (repo *secretRepository) Save(spec models.ProjectSecretItem) error {
	existingResource, err := repo.GetByName(spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find secret by name")
	}
	resource, err := Secret{}.FromSpec(spec, repo.project, repo.hash)
	if err != nil {
		return err
	}
	if err == nil {
		resource.ID = existingResource.ID
	}
	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *secretRepository) GetByName(name string) (models.ProjectSecretItem, error) {
	var r Secret
	if err := repo.db.Where("name = ? AND project_id = ?", name, repo.project.ID).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSecretItem{}, store.ErrResourceNotFound
		}
		return models.ProjectSecretItem{}, err
	}
	return r.ToSpec(repo.hash)
}

func (repo *secretRepository) GetByID(id uuid.UUID) (models.ProjectSecretItem, error) {
	var r Secret
	if err := repo.db.Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSecretItem{}, store.ErrResourceNotFound
		}
		return models.ProjectSecretItem{}, err
	}
	return r.ToSpec(repo.hash)
}

func (repo *secretRepository) GetAll() ([]models.ProjectSecretItem, error) {
	specs := []models.ProjectSecretItem{}
	resources := []Secret{}
	if err := repo.db.Find(&resources).Error; err != nil {
		return specs, err
	}
	for _, res := range resources {
		adapted, err := res.ToSpec(repo.hash)
		if err != nil {
			return specs, errors.Wrap(err, "failed to adapt secret")
		}
		specs = append(specs, adapted)
	}
	return specs, nil
}

func NewSecretRepository(db *gorm.DB, project models.ProjectSpec, hash models.ApplicationKey) *secretRepository {
	return &secretRepository{
		db:      db,
		project: project,
		hash:    hash,
	}
}
