package resource

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	backupToStoreColumns = `store, project_name, namespace_name, description, resource_names, config, created_at, updated_at`
	backupColumns        = `id, ` + backupToStoreColumns
)

type Backup struct {
	ID uuid.UUID

	Store         string
	ProjectName   string
	NamespaceName string

	Description   string
	ResourceNames pq.StringArray

	Config map[string]string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewBackup(b *resource.Backup) Backup {
	return Backup{
		ResourceNames: b.ResourceNames(),
		Store:         b.Store().String(),
		ProjectName:   b.Tenant().ProjectName().String(),
		NamespaceName: b.Tenant().NamespaceName().String(),
		Description:   b.Description(),
		CreatedAt:     b.CreatedAt(),
		Config:        b.Config(),
	}
}

func (b Backup) ToResourceBackup() (*resource.Backup, error) {
	s, err := resource.FromStringToStore(b.Store)
	if err != nil {
		return nil, err
	}
	tnnt, err := tenant.NewTenant(b.ProjectName, b.NamespaceName)
	if err != nil {
		return nil, err
	}

	backup, err := resource.NewBackup(s, tnnt, b.ResourceNames, b.Description, b.CreatedAt.UTC(), b.Config)
	if err != nil {
		return nil, err
	}

	err = backup.UpdateID(b.ID)
	if err != nil {
		return nil, err
	}

	return backup, nil
}

type BackupRepository struct {
	db *pgxpool.Pool
}

func (repo BackupRepository) Create(ctx context.Context, resourceBackup *resource.Backup) error {
	backup := NewBackup(resourceBackup)

	insertBackup := `INSERT INTO backup (` + backupToStoreColumns + `) VALUES ($1, $2, $3, $4, $5, $6, $7, now()) returning id`
	err := repo.db.QueryRow(ctx, insertBackup, backup.Store, backup.ProjectName, backup.NamespaceName,
		backup.Description, backup.ResourceNames, backup.Config, backup.CreatedAt).Scan(&backup.ID)

	if err != nil {
		return errors.Wrap(resource.EntityBackup, "unable to save backup in db", err)
	}

	return resourceBackup.UpdateID(backup.ID)
}

func (repo BackupRepository) GetByID(ctx context.Context, id resource.BackupID) (*resource.Backup, error) {
	var b Backup
	getByID := `SELECT ` + backupColumns + ` FROM backup WHERE id = $1`
	err := repo.db.QueryRow(ctx, getByID, id.String()).
		Scan(&b.ID, &b.Store, &b.ProjectName, &b.NamespaceName,
			&b.Description, &b.ResourceNames, &b.Config, &b.CreatedAt, &b.UpdatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(resource.EntityBackup, "record not found for id "+id.String())
		}
		return nil, errors.Wrap(resource.EntityBackup, "error while getting backup for id "+id.String(), err)
	}

	return b.ToResourceBackup()
}

func (repo BackupRepository) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error) {
	getAllBackups := `SELECT ` + backupColumns + ` FROM backup WHERE project_name = $1 AND namespace_name = $2 AND store = $3`
	rows, err := repo.db.Query(ctx, getAllBackups, tnnt.ProjectName(), tnnt.NamespaceName(), store)
	if err != nil {
		return nil, errors.Wrap(resource.EntityBackup, "error while getting backup", err)
	}
	defer rows.Close()

	var backups []*resource.Backup
	for rows.Next() {
		var b Backup
		err = rows.Scan(&b.ID, &b.Store, &b.ProjectName, &b.NamespaceName,
			&b.Description, &b.ResourceNames, &b.Config, &b.CreatedAt, &b.UpdatedAt)
		if err != nil {
			return nil, err
		}

		resourceBackup, err := b.ToResourceBackup()
		if err != nil {
			return nil, err
		}
		backups = append(backups, resourceBackup)
	}

	return backups, nil
}

func NewBackupRepository(pool *pgxpool.Pool) *BackupRepository {
	return &BackupRepository{db: pool}
}
