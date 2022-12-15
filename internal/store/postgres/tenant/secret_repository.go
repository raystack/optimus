package tenant

import (
	"context"
	"database/sql"
	"encoding/base64"
	"time"

	"github.com/google/uuid"
	"github.com/gtank/cryptopasta"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/dto"
	"github.com/odpf/optimus/internal/errors"
)

type SecretRepository struct {
	pool *pgxpool.Pool
}

const (
	secretColumns = `id, name, value, type, project_name, namespace_name, created_at, updated_at`

	getAllSecretsInProject = `SELECT ` + secretColumns + `
FROM secret s WHERE project_name = $1`
)

type Secret struct {
	ID uuid.UUID

	Name  string
	Value string

	Type string

	ProjectName   string
	NamespaceName sql.NullString

	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewSecret(secret *tenant.Secret) Secret {
	// base64 for storing safely in db
	base64cipher := base64.StdEncoding.EncodeToString([]byte(secret.EncodedValue()))

	nsName := sql.NullString{}
	if secret.NamespaceName() != "" {
		nsName = sql.NullString{String: secret.NamespaceName(), Valid: true}
	}
	return Secret{
		Name:          secret.Name().String(),
		Value:         base64cipher,
		Type:          secret.Type().String(),
		ProjectName:   secret.ProjectName().String(),
		NamespaceName: nsName,
	}
}

func (s Secret) ToTenantSecret() (*tenant.Secret, error) {
	// decode base64
	encrypted, err := base64.StdEncoding.DecodeString(s.Value)
	if err != nil {
		return nil, err
	}

	projName, err := tenant.ProjectNameFrom(s.ProjectName)
	if err != nil {
		return nil, err
	}

	typ, err := tenant.SecretTypeFromString(s.Type)
	if err != nil {
		return nil, err
	}

	nsName := ""
	if s.NamespaceName.Valid {
		nsName = s.NamespaceName.String
	}

	return tenant.NewSecret(s.Name, typ, string(encrypted), projName, nsName)
}

func (s Secret) ToSecretInfo() (*dto.SecretInfo, error) {
	encrypted, err := base64.StdEncoding.DecodeString(s.Value)
	if err != nil {
		return nil, err
	}

	digest := cryptopasta.Hash("user defined secrets", encrypted)
	base64encoded := base64.StdEncoding.EncodeToString(digest)

	typ, err := tenant.SecretTypeFromString(s.Type)
	if err != nil {
		return nil, err
	}

	nsName := ""
	if s.NamespaceName.Valid {
		nsName = s.NamespaceName.String
	}

	return &dto.SecretInfo{
		Name:      s.Name,
		Digest:    base64encoded,
		Type:      typ,
		Namespace: nsName,
		UpdatedAt: s.UpdatedAt,
	}, nil
}

func (s SecretRepository) Save(ctx context.Context, tenantSecret *tenant.Secret) error {
	secret := NewSecret(tenantSecret)

	err := s.get(ctx, tenantSecret.ProjectName(), tenantSecret.Name())
	if err == nil {
		return errors.NewError(errors.ErrAlreadyExists, tenant.EntitySecret, "secret already exists")
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return errors.Wrap(tenant.EntitySecret, "unable to save secret", err)
	}

	insertSecret := `INSERT INTO secret (name, value, type, project_name, namespace_name, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`
	_, err = s.pool.Exec(ctx, insertSecret, secret.Name, secret.Value, secret.Type, secret.ProjectName, secret.NamespaceName)

	if err != nil {
		return errors.Wrap(tenant.EntitySecret, "unable to save secret", err)
	}

	return nil
}

func (s SecretRepository) Update(ctx context.Context, tenantSecret *tenant.Secret) error {
	secret := NewSecret(tenantSecret)

	err := s.get(ctx, tenantSecret.ProjectName(), tenantSecret.Name())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NotFound(tenant.EntitySecret, "unable to update, secret not found for "+tenantSecret.Name().String())
		}
		return errors.Wrap(tenant.EntitySecret, "unable to update secret", err)
	}

	updateSecret := `UPDATE secret SET value=$1, type=$2, updated_at=NOW()
WHERE project_name = $3 AND name=$4`

	_, err = s.pool.Exec(ctx, updateSecret, secret.Value, secret.Type, secret.ProjectName, secret.Name)
	if err != nil {
		return errors.Wrap(tenant.EntitySecret, "unable to update secret", err)
	}
	return nil
}

// Get is scoped to the tenant provided in the argument
func (s SecretRepository) Get(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) (*tenant.Secret, error) {
	var secret Secret

	getSecretByNameQuery := `SELECT ` + secretColumns + `
FROM secret s WHERE name = $1
AND project_name = $2
AND (namespace_name IS NULL OR namespace_name = $3)`

	err := s.pool.QueryRow(ctx, getSecretByNameQuery, name, projName, nsName).
		Scan(&secret.ID, &secret.Name, &secret.Value, &secret.Type,
			&secret.ProjectName, &secret.NamespaceName, &secret.CreatedAt, &secret.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(tenant.EntitySecret, "no record for "+name.String())
		}
		return nil, errors.Wrap(tenant.EntitySecret, "error while getting secret", err)
	}

	return secret.ToTenantSecret()
}

// get is scoped only at project level, used for db operations
func (s SecretRepository) get(ctx context.Context, projName tenant.ProjectName, name tenant.SecretName) error {
	var dummyName string
	getSecretByNameAtProject := `SELECT s.name FROM secret s WHERE name = $1 AND project_name = $2`
	err := s.pool.QueryRow(ctx, getSecretByNameAtProject, name, projName).Scan(&dummyName)
	return err
}

func (s SecretRepository) GetAll(ctx context.Context, projName tenant.ProjectName, nsName string) ([]*tenant.Secret, error) {
	var queryErr error
	var rows pgx.Rows

	if nsName != "" {
		getAllSecretsAvailableForNamespace := `SELECT ` + secretColumns + ` FROM secret
WHERE project_name = $1 AND (namespace_name IS NULL or namespace_name = $2)`
		rows, queryErr = s.pool.Query(ctx, getAllSecretsAvailableForNamespace, projName, nsName)
	} else {
		rows, queryErr = s.pool.Query(ctx, getAllSecretsInProject, projName)
	}

	if queryErr != nil {
		return nil, errors.Wrap(tenant.EntitySecret, "unable to get all secrets in scope", queryErr)
	}
	defer rows.Close()

	var tenantSecrets []*tenant.Secret
	for rows.Next() {
		var sec Secret
		err := rows.Scan(&sec.ID, &sec.Name, &sec.Value, &sec.Type,
			&sec.ProjectName, &sec.NamespaceName, &sec.CreatedAt, &sec.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(tenant.EntitySecret, "error in GetAll", err)
		}

		secret, err := sec.ToTenantSecret()
		if err != nil {
			return nil, err
		}
		tenantSecrets = append(tenantSecrets, secret)
	}

	return tenantSecrets, nil
}

// Delete will not support soft delete, once deleted it has to be created again
func (s SecretRepository) Delete(ctx context.Context, projName tenant.ProjectName, nsName string, name tenant.SecretName) error {
	var result pgconn.CommandTag
	var err error
	if nsName != "" {
		deleteForNamespaceScope := `DELETE FROM secret
WHERE name = $1 AND project_name = $2 AND namespace_name = $3`
		result, err = s.pool.Exec(ctx, deleteForNamespaceScope, name, projName, nsName)
	} else {
		deleteForProjectScope := `DELETE FROM secret
WHERE project_name = $1 AND name = $2 AND namespace_name IS NULL`
		result, err = s.pool.Exec(ctx, deleteForProjectScope, projName, name)
	}

	if err != nil {
		return errors.Wrap(tenant.EntitySecret, "error during delete of secret", err)
	}

	if result.RowsAffected() == 0 {
		return errors.NotFound(tenant.EntitySecret, "secret to delete not found "+name.String())
	}
	return nil
}

func (s SecretRepository) GetSecretsInfo(ctx context.Context, projName tenant.ProjectName) ([]*dto.SecretInfo, error) {
	rows, err := s.pool.Query(ctx, getAllSecretsInProject, projName)

	if err != nil {
		return nil, errors.Wrap(tenant.EntitySecret, "unable to get all secrets info", err)
	}
	defer rows.Close()

	var secretInfo []*dto.SecretInfo
	for rows.Next() {
		var sec Secret
		err := rows.Scan(&sec.ID, &sec.Name, &sec.Value, &sec.Type,
			&sec.ProjectName, &sec.NamespaceName, &sec.CreatedAt, &sec.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(tenant.EntitySecret, "error in GetAll", err)
		}

		secret, err := sec.ToSecretInfo()
		if err != nil {
			return nil, err
		}
		secretInfo = append(secretInfo, secret)
	}

	return secretInfo, nil
}

func NewSecretRepository(pool *pgxpool.Pool) *SecretRepository {
	return &SecretRepository{pool: pool}
}
