package postgres

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/gtank/cryptopasta"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type Secret struct {
	ID        uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	NamespaceID uuid.UUID `gorm:"default:null"`
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	Name  string `gorm:"not null"`
	Value string

	Type string

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func (p Secret) FromSpec(spec models.ProjectSecretItem, proj models.ProjectSpec, namespace models.NamespaceSpec,
	hash models.ApplicationKey) (Secret, error) {
	// encrypt secret
	cipher, err := cryptopasta.Encrypt([]byte(spec.Value), hash.GetKey())
	if err != nil {
		return Secret{}, err
	}

	// base64 for storing safely in db
	base64cipher := base64.StdEncoding.EncodeToString(cipher)

	secretType := models.SecretTypeUserDefined
	if strings.HasPrefix(spec.Name, models.SecretTypeSystemDefinedPrefix) {
		secretType = models.SecretTypeSystemDefined
	}

	return Secret{
		ID:          spec.ID,
		Name:        spec.Name,
		Value:       base64cipher,
		ProjectID:   proj.ID,
		NamespaceID: namespace.ID,
		Type:        secretType.String(),
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

	secretType := models.SecretTypeSystemDefined
	if p.Type == models.SecretTypeUserDefined.String() {
		secretType = models.SecretTypeUserDefined
	}

	return models.ProjectSecretItem{
		ID:    p.ID,
		Name:  p.Name,
		Value: string(cleartext),
		Type:  secretType,
	}, nil
}

func (p Secret) ToSecretItemInfo(hash models.ApplicationKey) (models.SecretItemInfo, error) {
	// decode base64
	encrypted, err := base64.StdEncoding.DecodeString(p.Value)
	if err != nil {
		return models.SecretItemInfo{}, err
	}

	digest := cryptopasta.Hash("user defined secrets", encrypted)
	base64encoded := base64.StdEncoding.EncodeToString(digest)

	secretType := models.SecretTypeSystemDefined
	if p.Type == models.SecretTypeUserDefined.String() {
		secretType = models.SecretTypeUserDefined
	}

	return models.SecretItemInfo{
		ID:        p.ID,
		Name:      p.Name,
		Digest:    base64encoded,
		Type:      secretType,
		Namespace: p.Namespace.Name,
		UpdatedAt: p.UpdatedAt,
	}, nil
}

type secretRepository struct {
	db      *gorm.DB
	project models.ProjectSpec

	hash models.ApplicationKey
}

func (repo *secretRepository) Insert(ctx context.Context, namespace models.NamespaceSpec, resource models.ProjectSecretItem) error {
	p, err := Secret{}.FromSpec(resource, repo.project, namespace, repo.hash)
	if err != nil {
		return err
	}
	if len(p.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.WithContext(ctx).Save(&p).Error
}

func (repo *secretRepository) Save(ctx context.Context, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	_, err := repo.GetByName(ctx, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, namespace, spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find secret by name")
	}
	return errors.New("secret already exist")
}

func (repo *secretRepository) Update(ctx context.Context, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	existingResource, err := repo.GetByName(ctx, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return errors.New(fmt.Sprintf("secret %s does not exist", spec.Name))
	} else if err != nil {
		return errors.Wrap(err, "unable to find secret by name")
	}

	resource, err := Secret{}.FromSpec(spec, repo.project, namespace, repo.hash)
	if err != nil {
		return err
	}
	if err == nil {
		resource.ID = existingResource.ID
	}
	return repo.db.WithContext(ctx).Model(&resource).Updates(&resource).Error
}

func (repo *secretRepository) GetByName(ctx context.Context, name string) (models.ProjectSecretItem, error) {
	var r Secret
	if err := repo.db.WithContext(ctx).Where("name = ? AND project_id = ?", name, repo.project.ID).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSecretItem{}, store.ErrResourceNotFound
		}
		return models.ProjectSecretItem{}, err
	}
	return r.ToSpec(repo.hash)
}

func (repo *secretRepository) GetByID(ctx context.Context, id uuid.UUID) (models.ProjectSecretItem, error) {
	var r Secret
	if err := repo.db.WithContext(ctx).Where("id = ?", id).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSecretItem{}, store.ErrResourceNotFound
		}
		return models.ProjectSecretItem{}, err
	}
	return r.ToSpec(repo.hash)
}

func (repo *secretRepository) GetAll(ctx context.Context) ([]models.SecretItemInfo, error) {
	var secretItems []models.SecretItemInfo
	var resources []Secret
	if err := repo.db.WithContext(ctx).Preload("Namespace").
		Joins("LEFT JOIN namespace ON secret.namespace_id = namespace.id").
		Where("secret.project_id = ? and secret.type = ?", repo.project.ID, models.SecretTypeUserDefined).Find(&resources).Error; err != nil {
		return secretItems, err
	}
	for _, res := range resources {
		adapted, err := res.ToSecretItemInfo(repo.hash)
		if err != nil {
			return secretItems, errors.Wrap(err, "failed to adapt secret")
		}
		secretItems = append(secretItems, adapted)
	}

	return secretItems, nil
}

func NewSecretRepository(db *gorm.DB, project models.ProjectSpec, hash models.ApplicationKey) *secretRepository {
	return &secretRepository{
		db:      db,
		project: project,
		hash:    hash,
	}
}
