package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gtank/cryptopasta"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Secret struct {
	ID        uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	NamespaceID uuid.UUID `gorm:"default:null"`
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	Name  string `gorm:"not null;default:null"`
	Value string

	Type string

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func (Secret) FromSpec(spec models.ProjectSecretItem, proj models.ProjectSpec, namespace models.NamespaceSpec,
	hash models.ApplicationKey) (Secret, error) {
	// encrypt secret, TODO: What to do when the Value is empty ?
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
		ProjectID:   proj.ID.UUID(),
		NamespaceID: namespace.ID,
		Type:        secretType.String(),
	}, nil
}

// ToSpec TODO: move decryption of secret to service
func (p Secret) ToSpec(key models.ApplicationKey) (models.ProjectSecretItem, error) {
	// decode base64
	encrypted, err := base64.StdEncoding.DecodeString(p.Value)
	if err != nil {
		return models.ProjectSecretItem{}, err
	}

	// decrypt secret
	cleartext, err := cryptopasta.Decrypt(encrypted, key.GetKey())
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

func (p Secret) ToSecretItemInfo() (models.SecretItemInfo, error) {
	// decode base64
	encrypted, err := base64.StdEncoding.DecodeString(p.Value)
	if err != nil {
		return models.SecretItemInfo{}, err
	}

	digest := cryptopasta.Hash("user defined secrets", encrypted)
	base64encoded := base64.StdEncoding.EncodeToString(digest)

	// Todo: Move to Secret type
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
	db *gorm.DB

	hash models.ApplicationKey
}

func (repo *secretRepository) Insert(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, resource models.ProjectSecretItem) error {
	p, err := Secret{}.FromSpec(resource, project, namespace, repo.hash)
	if err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Save(&p).Error
}

func (repo *secretRepository) Save(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	_, err := repo.GetByName(ctx, project, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, project, namespace, spec)
	} else if err != nil {
		return fmt.Errorf("unable to find secret by name: %w", err)
	}
	return store.ErrResourceExists
}

func (repo *secretRepository) Update(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, spec models.ProjectSecretItem) error {
	existingResource, err := repo.GetByName(ctx, project, spec.Name)
	if err != nil {
		return err
	}

	resource, err := Secret{}.FromSpec(spec, project, namespace, repo.hash)
	if err != nil {
		return err
	}

	resource.ID = existingResource.ID
	return repo.db.WithContext(ctx).Model(&resource).Updates(&resource).Error
}

func (repo *secretRepository) GetByName(ctx context.Context, project models.ProjectSpec, name string) (models.ProjectSecretItem, error) {
	var r Secret
	if err := repo.db.WithContext(ctx).
		Where("project_id = ?", project.ID.UUID()).
		Where("name = ?", name).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ProjectSecretItem{}, store.ErrResourceNotFound
		}
		return models.ProjectSecretItem{}, err
	}
	return r.ToSpec(repo.hash)
}

func (repo *secretRepository) GetAll(ctx context.Context, project models.ProjectSpec) ([]models.SecretItemInfo, error) {
	var secretItems []models.SecretItemInfo
	var resources []Secret
	if err := repo.db.WithContext(ctx).Preload("Namespace").
		Joins("LEFT JOIN namespace ON secret.namespace_id = namespace.id").
		Where("secret.project_id = ?", project.ID.UUID()).
		Where("secret.type = ?", models.SecretTypeUserDefined).
		Find(&resources).Error; err != nil {
		return secretItems, err
	}
	for _, res := range resources {
		adapted, err := res.ToSecretItemInfo()
		if err != nil {
			return secretItems, fmt.Errorf("failed to adapt secret: %w", err)
		}
		secretItems = append(secretItems, adapted)
	}

	return secretItems, nil
}

func (repo secretRepository) GetSecrets(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec) ([]models.ProjectSecretItem, error) {
	var secretItems []models.ProjectSecretItem
	var resources []Secret
	if err := repo.db.WithContext(ctx).
		Where("project_id = ?", project.ID.UUID()).
		Where("type = ?", models.SecretTypeUserDefined).
		Where("namespace_id is null or namespace_id = ?", namespace.ID).
		Find(&resources).Error; err != nil {
		return secretItems, err
	}
	for _, res := range resources {
		adapted, err := res.ToSpec(repo.hash)
		if err != nil {
			return secretItems, fmt.Errorf("failed to adapt secret: %w", err)
		}
		secretItems = append(secretItems, adapted)
	}

	return secretItems, nil
}

func (repo *secretRepository) Delete(ctx context.Context, project models.ProjectSpec, namespace models.NamespaceSpec, secretName string) error {
	query := repo.db.WithContext(ctx).
		Where("project_id = ?", project.ID.UUID()).
		Where("name = ?", secretName)

	var result *gorm.DB
	if namespace.Name == "" {
		result = query.Where("namespace_id is null").Delete(&Secret{})
	} else {
		result = query.Where("namespace_id = ?", namespace.ID).Delete(&Secret{})
	}

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return store.ErrResourceNotFound
	}

	return nil
}

func NewSecretRepository(db *gorm.DB, hash models.ApplicationKey) *secretRepository {
	return &secretRepository{
		db:   db,
		hash: hash,
	}
}
