package tenant

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/google/uuid"
	"github.com/gtank/cryptopasta"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/dto"
	"github.com/odpf/optimus/internal/errors"
)

type SecretRepository struct {
	db *gorm.DB
}

const (
	secretColumns = `s.id, s.name, s.value, s.type, p.name as project_name, n.name as namespace_name, s.created_at, s.updated_at`

	getAllSecretsInProject = `SELECT ` + secretColumns + `
FROM secret s 
JOIN project p ON p.id = s.project_id
LEFT JOIN namespace n ON n.id = s.namespace_id
WHERE p.name = ?`

	secretCTE = `WITH cte_tenant AS (
SELECT p.id AS project_id, p.name AS project_name, n.id AS namespace_id, n.name AS namespace_name 
FROM project p
	LEFT JOIN namespace n
		ON p.id = n.project_id and n.name = ?
WHERE p.name = ?
) `
)

type Secret struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	Name  string `gorm:"not null;default:null"`
	Value string

	Type string

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func NewSecret(secret *tenant.Secret) Secret {
	nsName := ""
	if ns, err := secret.Tenant().NamespaceName(); err == nil {
		nsName = ns.String()
	}

	// base64 for storing safely in db
	base64cipher := base64.StdEncoding.EncodeToString([]byte(secret.EncodedValue()))

	return Secret{
		Name:          secret.Name().String(),
		Value:         base64cipher,
		Type:          secret.Type().String(),
		ProjectName:   secret.Tenant().ProjectName().String(),
		NamespaceName: nsName,
	}
}

func (s Secret) ToTenantSecret() (*tenant.Secret, error) {
	// decode base64
	encrypted, err := base64.StdEncoding.DecodeString(s.Value)
	if err != nil {
		return nil, err
	}

	t, err := tenant.NewTenant(s.ProjectName, s.NamespaceName)
	if err != nil {
		return nil, err
	}

	typ, err := tenant.SecretTypeFromString(s.Type)
	if err != nil {
		return nil, err
	}

	return tenant.NewSecret(s.Name, typ, string(encrypted), t)
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

	return &dto.SecretInfo{
		Name:      s.Name,
		Digest:    base64encoded,
		Type:      typ,
		Namespace: s.NamespaceName,
		UpdatedAt: s.UpdatedAt,
	}, nil
}

func (s SecretRepository) Save(ctx context.Context, t tenant.Tenant, tenantSecret *tenant.Secret) error {
	secret := NewSecret(tenantSecret)

	_, err := s.get(ctx, t, tenantSecret.Name())
	if err == nil {
		return errors.NewError(errors.ErrAlreadyExists, tenant.EntitySecret, "secret already exists")
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.Wrap(tenant.EntitySecret, "unable to save secret", err)
	}

	insertSecret := secretCTE + `INSERT INTO secret (name, value, type, project_id, namespace_id, updated_at, created_at)
SELECT ?, ?, ?, t.project_id, t.namespace_id, NOW(), NOW()
FROM cte_tenant t`

	result := s.db.WithContext(ctx).Exec(insertSecret, secret.NamespaceName, secret.ProjectName,
		secret.Name, secret.Value, secret.Type)

	if result.Error != nil {
		return errors.Wrap(tenant.EntitySecret, "unable to save secret", err)
	}

	if result.RowsAffected == 0 {
		return errors.InternalError(tenant.EntitySecret, "unable to save, rows affected 0", nil)
	}
	return nil
}

func (s SecretRepository) Update(ctx context.Context, t tenant.Tenant, tenantSecret *tenant.Secret) error {
	secret := NewSecret(tenantSecret)

	_, err := s.get(ctx, t, tenantSecret.Name())
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.NotFound(tenant.EntitySecret, "unable to update, secret not found for "+tenantSecret.Name().String())
		}
		return errors.Wrap(tenant.EntitySecret, "unable to update secret", err)
	}

	updateSecret := `UPDATE secret
SET value=?, type=?, updated_at=NOW()
FROM secret s
    JOIN project p
        ON p.id = s.project_id
WHERE p.name = ? AND s.name=?`

	err = s.db.WithContext(ctx).Exec(updateSecret, secret.Value, secret.Type, secret.ProjectName, secret.Name).Error
	if err != nil {
		return errors.Wrap(tenant.EntitySecret, "unable to update secret", err)
	}
	return nil
}

// Get is scoped to the tenant provided in the argument
func (s SecretRepository) Get(ctx context.Context, t tenant.Tenant, name tenant.SecretName) (*tenant.Secret, error) {
	var secret Secret
	namespaceName := ""
	if ns, err := t.NamespaceName(); err == nil {
		namespaceName = ns.String()
	}

	getSecretByNameQuery := secretCTE + `SELECT s.id, s.name, s.value, s.type, t.project_name, t.namespace_name, s.created_at, s.updated_at
FROM secret s
    JOIN cte_tenant t
        ON t.project_id = s.project_id
        AND (t.namespace_id IS NULL OR s.namespace_id IS NULL OR t.namespace_id = s.namespace_id )
WHERE s.name = ?`
	err := s.db.WithContext(ctx).Raw(getSecretByNameQuery, namespaceName, t.ProjectName().String(), name).
		First(&secret).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(tenant.EntitySecret, "no record for "+name.String())
		}
		return nil, errors.Wrap(tenant.EntitySecret, "error while getting project", err)
	}

	return secret.ToTenantSecret()
}

// get is scoped only at project level, used for db operations
func (s SecretRepository) get(ctx context.Context, t tenant.Tenant, name tenant.SecretName) (Secret, error) { // nolint: unparam
	var secret Secret

	getSecretByNameAtProject := `SELECT s.name
FROM secret s
JOIN project p
ON p.id = s.project_id
WHERE s.name = ?
AND p.name = ?
`
	err := s.db.WithContext(ctx).Raw(getSecretByNameAtProject, name.String(), t.ProjectName().String()).
		First(&secret).Error

	return secret, err
}

func (s SecretRepository) GetAll(ctx context.Context, t tenant.Tenant) ([]*tenant.Secret, error) {
	var secrets []Secret
	var queryErr error

	if nsName, err := t.NamespaceName(); err == nil {
		getAllSecretsAvailableForNamespace := `SELECT ` + secretColumns + `
FROM secret s
	JOIN project p ON p.id = s.project_id
	LEFT JOIN namespace n ON n.id = s.namespace_id
WHERE p.name = ?
AND (s.namespace_id IS NULL or n.name = ?)`
		queryErr = s.db.WithContext(ctx).Raw(getAllSecretsAvailableForNamespace, t.ProjectName().String(), nsName.String()).
			Scan(&secrets).Error
	} else {
		queryErr = s.db.WithContext(ctx).Raw(getAllSecretsInProject, t.ProjectName().String()).
			Scan(&secrets).Error
	}
	if queryErr != nil {
		return nil, errors.Wrap(tenant.EntitySecret, "unable to get all secrets in scope", queryErr)
	}

	tenantSecrets := make([]*tenant.Secret, len(secrets))
	for i, secret := range secrets {
		tenantSecret, err := secret.ToTenantSecret()
		if err != nil {
			return nil, err
		}
		tenantSecrets[i] = tenantSecret
	}

	return tenantSecrets, nil
}

// Delete will not support soft delete, once deleted it has to be created again
func (s SecretRepository) Delete(ctx context.Context, t tenant.Tenant, name tenant.SecretName) error {
	var result *gorm.DB
	if ns, err := t.NamespaceName(); err == nil {
		deleteForNamespaceScope := secretCTE + `DELETE
FROM secret s
USING cte_tenant t
WHERE s.name = ?
AND s.project_id = t.project_id
AND s.namespace_id = t.namespace_id`
		result = s.db.WithContext(ctx).Exec(deleteForNamespaceScope, ns.String(), t.ProjectName().String(), name.String())
	} else {
		deleteForProjectScope := `DELETE
FROM secret s
USING project p
WHERE p.name = ?
AND s.name = ?
AND s.project_id = p.id
AND s.namespace_id IS NULL`
		result = s.db.WithContext(ctx).Exec(deleteForProjectScope, t.ProjectName().String(), name)
	}

	if result.Error != nil {
		return errors.Wrap(tenant.EntitySecret, "error during delete of secret", result.Error)
	}

	if result.RowsAffected == 0 {
		return errors.NotFound(tenant.EntitySecret, "secret to delete not found "+name.String())
	}
	return nil
}

func (s SecretRepository) GetSecretsInfo(ctx context.Context, t tenant.Tenant) ([]*dto.SecretInfo, error) {
	var secrets []Secret
	if err := s.db.WithContext(ctx).Raw(getAllSecretsInProject, t.ProjectName().String()).
		Scan(&secrets).Error; err != nil {
		return nil, errors.Wrap(tenant.EntitySecret, "unable to get information about secrets", err)
	}

	infos := make([]*dto.SecretInfo, len(secrets))
	for i, secret := range secrets {
		info, err := secret.ToSecretInfo()
		if err != nil {
			return nil, err
		}
		infos[i] = info
	}

	return infos, nil
}

func NewSecretRepository(db *gorm.DB) *SecretRepository {
	return &SecretRepository{db: db}
}
