package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type Backup struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	Store         string
	ProjectName   string
	NamespaceName string

	Description   string
	ResourceNames pq.StringArray `gorm:"type:text[]"`

	Config datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func NewBackup(b *resource.Backup) (Backup, error) {
	conf, err := json.Marshal(b.Config())
	if err != nil {
		return Backup{}, err
	}

	return Backup{
		ResourceNames: b.ResourceNames(),
		Store:         b.Store().String(),
		ProjectName:   b.Tenant().ProjectName().String(),
		NamespaceName: b.Tenant().NamespaceName().String(),
		Description:   b.Description(),
		CreatedAt:     b.CreatedAt(),
		Config:        conf,
	}, nil
}

func (b Backup) ToResourceBackup() (*resource.Backup, error) {
	s, err := resource.FromStringToStore(b.Store)
	if err != nil {
		fmt.Printf("Error in resource backup: %v\n", err)
		return nil, err
	}
	tnnt, err := tenant.NewTenant(b.ProjectName, b.NamespaceName)
	if err != nil {
		fmt.Printf("Error in resource backup: %v\n", err)
		return nil, err
	}

	var config map[string]string
	if err = json.Unmarshal(b.Config, &config); err != nil {
		fmt.Printf("Error in resource backup: %v\n", err)
		return nil, errors.Wrap(resource.EntityBackup, "error unmarshalling config", err)
	}

	backup, err := resource.NewBackup(s, tnnt, b.ResourceNames, b.Description, b.CreatedAt.UTC(), config)
	if err != nil {
		fmt.Printf("Error in resource backup: %v\n", err)
		return nil, err
	}

	err = backup.UpdateID(b.ID)
	if err != nil {
		return nil, err
	}

	return backup, nil
}

type BackupRepository struct {
	db *gorm.DB
}

func (repo BackupRepository) Create(ctx context.Context, resourceBackup *resource.Backup) error {
	backup, err := NewBackup(resourceBackup)
	if err != nil {
		return err
	}

	if err = repo.db.WithContext(ctx).Create(&backup).Error; err != nil {
		return errors.Wrap(resource.EntityBackup, "unable to save backup in db", err)
	}

	return resourceBackup.UpdateID(backup.ID)
}

func (repo BackupRepository) GetByID(ctx context.Context, id resource.BackupID) (*resource.Backup, error) {
	var b Backup
	if err := repo.db.WithContext(ctx).
		Where("id = ?", id.UUID()).First(&b).Error; err != nil {
		fmt.Println(err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(resource.EntityBackup, "record not found for id "+id.String())
		}
		return nil, errors.Wrap(resource.EntityBackup, "error while getting backup for id "+id.String(), err)
	}

	return b.ToResourceBackup()
}

func (repo BackupRepository) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error) {
	var backups []Backup
	err := repo.db.WithContext(ctx).Where("project_name = ?", tnnt.ProjectName().String()).
		Where("namespace_name = ?", tnnt.NamespaceName().String()).
		Where("store = ?", store.String()).
		Find(&backups).Error
	if err != nil {
		return nil, errors.Wrap(resource.EntityBackup, "error while getting backup", err)
	}

	var resourceBackups []*resource.Backup
	for _, backup := range backups {
		resourceBackup, err := backup.ToResourceBackup()
		if err != nil {
			return nil, err
		}
		resourceBackups = append(resourceBackups, resourceBackup)
	}

	return resourceBackups, nil
}

func NewBackupRepository(db *gorm.DB) *BackupRepository {
	return &BackupRepository{db: db}
}
