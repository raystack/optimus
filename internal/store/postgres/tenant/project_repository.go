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

type ProjectRepository struct {
	db *gorm.DB
}

const (
	projectColumns        = `id, name, config, created_at, updated_at`
	getProjectByNameQuery = `select ` + projectColumns + ` from project where name = ? and deleted_at is null`
	getAllProjects        = `select ` + projectColumns + ` from project where deleted_at is null`

	insertProjectQuery = `insert into project (name, config, project_id, created_at, updated_at) values (?, ?, ?, now(), now())`
	updateProjectQuery = `update project set config=?, updated_at=now() where name=?`
)

type Project struct {
	ID     uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name   string    `gorm:"not null;unique"`
	Config datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func NewProject(spec *tenant.Project) (Project, error) {
	jsonBytes, err := json.Marshal(spec.GetConfigs())
	if err != nil {
		return Project{}, err
	}
	project := Project{
		Name:   spec.Name().String(),
		Config: jsonBytes,
	}
	return project, nil
}

func (p Project) ToTenantProject() (*tenant.Project, error) {
	var conf map[string]string
	err := json.Unmarshal(p.Config, &conf)
	if err != nil {
		return nil, err
	}
	return tenant.NewProject(p.Name, conf)
}

func (repo ProjectRepository) Save(ctx context.Context, tenantProject *tenant.Project) error {
	project, err := NewProject(tenantProject)
	if err != nil {
		return err
	}

	_, err = repo.get(ctx, tenantProject.Name())
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return repo.db.WithContext(ctx).Exec(insertProjectQuery, project.Name, project.Config).Error
		}
		return errors.Wrap(tenant.EntityProject, "unable to save project", err)
	}

	if len(tenantProject.GetConfigs()) == 0 { // TODO: project config cannot be empty
		return store.ErrEmptyConfig
	}
	return repo.db.WithContext(ctx).Exec(updateProjectQuery, project.Config, project.Name).Error
}

func (repo ProjectRepository) GetByName(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	project, err := repo.get(ctx, name)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(tenant.EntityProject, "no record for "+name.String())
		}
		return nil, errors.Wrap(tenant.EntityProject, "error while getting project", err)
	}
	return project.ToTenantProject()
}

func (repo ProjectRepository) get(ctx context.Context, name tenant.ProjectName) (Project, error) {
	var project Project

	err := repo.db.WithContext(ctx).Raw(getProjectByNameQuery, name.String()).First(&project).Error
	if err != nil {
		return Project{}, err
	}
	return project, nil
}

func (repo ProjectRepository) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	var projects []Project
	if err := repo.db.WithContext(ctx).Raw(getAllProjects).Scan(&projects).Error; err != nil {
		return nil, errors.Wrap(tenant.EntityProject, "error in GetAll", err)
	}

	var tenantProjects []*tenant.Project
	for _, proj := range projects {
		tenantProject, err := proj.ToTenantProject()
		if err != nil {
			return nil, err
		}
		tenantProjects = append(tenantProjects, tenantProject)
	}
	return tenantProjects, nil
}

func NewProjectRepository(db *gorm.DB) *ProjectRepository {
	return &ProjectRepository{
		db: db,
	}
}
