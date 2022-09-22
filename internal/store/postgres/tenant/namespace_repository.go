package tenant

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/store"
)

type NamespaceRepository struct {
	db *gorm.DB

	projectRepo ProjectRepository
}

const (
	namespaceColumns         = `n.id, n.name, n.config, p.name as project_name, n.created_at, n.updated_at`
	getNamespaceByNameQuery  = `select ` + namespaceColumns + ` from namespace n join project p on p.id = n.project_id where p.name = ? and n.name = ? and n.deleted_at is null`
	getAllNamespaceInProject = `select ` + namespaceColumns + ` from namespace n join project p on p.id = n.project_id where p.name = ? and n.deleted_at is null`

	insertNamespace      = `insert into namespace (name, config, project_id, updated_at, created_at) SELECT ?, ?, id, now(), now() FROM project p where p.name = ?;`
	updateNamespaceQuery = `update namespace set config=?, updated_at=now() From namespace n join project p on p.id = n.project_id  where p.name = ? and n.name=?`
)

type Namespace struct {
	ID     uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name   string    `gorm:"not null;unique"`
	Config datatypes.JSON

	ProjectName string `json:"project_name"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func NewNamespace(spec *tenant.Namespace) (Namespace, error) {
	jsonBytes, err := json.Marshal(spec.GetConfigs())
	if err != nil {
		return Namespace{}, err
	}
	namespace := Namespace{
		Name:        spec.Name().String(),
		ProjectName: spec.ProjectName().String(),
		Config:      jsonBytes,
	}
	return namespace, nil
}

func (n Namespace) ToTenantNamespace() (*tenant.Namespace, error) {
	var conf map[string]string
	err := json.Unmarshal(n.Config, &conf)
	if err != nil {
		return nil, err
	}
	projName, err := tenant.ProjectNameFrom(n.ProjectName)
	if err != nil {
		return nil, err
	}

	return tenant.NewNamespace(n.Name, projName, conf)
}

func (n *NamespaceRepository) Save(ctx context.Context, tenantNamespace *tenant.Namespace) error {
	namespace, err := NewNamespace(tenantNamespace)
	if err != nil {
		return errors.Wrap(tenant.EntityNamespace, "not able to convert namespace", err)
	}

	_, err = n.get(ctx, tenantNamespace.ProjectName(), tenantNamespace.Name())
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return n.db.WithContext(ctx).
				Exec(insertNamespace, namespace.Name, namespace.Config, namespace.ProjectName).Error
		}
		return errors.Wrap(tenant.EntityProject, "unable to save project", err)
	}

	if len(tenantNamespace.GetConfigs()) == 0 {
		return store.ErrEmptyConfig
	}
	return n.db.WithContext(ctx).
		Exec(updateNamespaceQuery, namespace.Config, namespace.ProjectName, namespace.Name).Error
}

func (n *NamespaceRepository) GetByName(ctx context.Context, projectName tenant.ProjectName, name tenant.NamespaceName) (*tenant.Namespace, error) {
	ns, err := n.get(ctx, projectName, name)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(tenant.EntityNamespace, "no record for "+name.String())
		}
		return nil, errors.Wrap(tenant.EntityNamespace, "error while getting project", err)
	}
	return ns.ToTenantNamespace()
}

func (n *NamespaceRepository) get(ctx context.Context, projName tenant.ProjectName, name tenant.NamespaceName) (Namespace, error) {
	var namespace Namespace

	err := n.db.WithContext(ctx).Raw(getNamespaceByNameQuery, projName.String(), name.String()).
		First(&namespace).Error
	if err != nil {
		return Namespace{}, err
	}
	return namespace, nil
}

func (n *NamespaceRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*tenant.Namespace, error) {
	var namespaces []Namespace

	err := n.db.WithContext(ctx).Raw(getAllNamespaceInProject, projectName.String()).
		Scan(&namespaces).Error
	if err != nil {
		return nil, errors.Wrap(tenant.EntityNamespace, "error in GetAll", err)
	}

	var tenantNamespace []*tenant.Namespace
	for _, ns := range namespaces {
		tenantNS, err := ns.ToTenantNamespace()
		if err != nil {
			return nil, err
		}
		tenantNamespace = append(tenantNamespace, tenantNS)
	}
	return tenantNamespace, nil
}

func NewNamespaceRepository(db *gorm.DB) *NamespaceRepository {
	return &NamespaceRepository{
		db: db,
	}
}
