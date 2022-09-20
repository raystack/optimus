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

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
)

type BackupDetail struct {
	Result      map[string]interface{}
	Description string
	Config      map[string]string
}

type Backup struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	ResourceID uuid.UUID
	Resource   Resource `gorm:"foreignKey:ResourceID"`

	Spec datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

type backupRepository struct {
	db *gorm.DB
}

func (Backup) FromSpec(backupSpec models.BackupSpec) (Backup, error) {
	adaptResource, err := Resource{}.FromSpec(backupSpec.Resource)
	if err != nil {
		return Backup{}, err
	}

	toDBSpec := BackupDetail{
		Result:      backupSpec.Result,
		Description: backupSpec.Description,
		Config:      backupSpec.Config,
	}
	specInBytes, err := json.Marshal(toDBSpec)
	if err != nil {
		return Backup{}, nil
	}

	return Backup{
		ID:         backupSpec.ID,
		ResourceID: adaptResource.ID,
		Spec:       specInBytes,
	}, nil
}

func (repo *backupRepository) Save(ctx context.Context, spec models.BackupSpec) error {
	if len(spec.Resource.ID) == 0 {
		return errors.New("resource cannot be empty")
	}
	p, err := Backup{}.FromSpec(spec)
	if err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Create(&p).Error
}

func (b Backup) ToSpec(ds models.Datastorer) (models.BackupSpec, error) {
	backupSpec := BackupDetail{}
	if err := json.Unmarshal(b.Spec, &backupSpec); err != nil {
		return models.BackupSpec{}, err
	}

	resourceSpec, err := b.Resource.ToSpec(ds)
	if err != nil {
		return models.BackupSpec{}, err
	}

	return models.BackupSpec{
		ID:          b.ID,
		Resource:    resourceSpec,
		Result:      backupSpec.Result,
		Description: backupSpec.Description,
		Config:      backupSpec.Config,
		CreatedAt:   b.CreatedAt,
	}, nil
}

func (repo *backupRepository) GetAll(ctx context.Context, project models.ProjectSpec, ds models.Datastorer) ([]models.BackupSpec, error) {
	var specs []models.BackupSpec
	var backups []Backup
	if err := repo.db.WithContext(ctx).Preload("Resource").Joins("JOIN resource ON backup.resource_id = resource.id").
		Where("resource.project_id = ?", project.ID.UUID()).Find(&backups).Error; err != nil {
		return specs, err
	}
	for _, b := range backups {
		adapted, err := b.ToSpec(ds)
		if err != nil {
			return specs, fmt.Errorf("failed to adapt backup: %w", err)
		}
		specs = append(specs, adapted)
	}
	return specs, nil
}

func (repo *backupRepository) GetByID(ctx context.Context, id uuid.UUID, ds models.Datastorer) (models.BackupSpec, error) {
	var b Backup
	if err := repo.db.WithContext(ctx).Preload("Resource").Joins("JOIN resource ON backup.resource_id = resource.id").
		Where("backup.id = ?", id).First(&b).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.BackupSpec{}, store.ErrResourceNotFound
		}
		return models.BackupSpec{}, err
	}
	return b.ToSpec(ds)
}

func NewBackupRepository(db *gorm.DB) *backupRepository {
	return &backupRepository{
		db: db,
	}
}
