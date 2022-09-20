package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/store"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type ProjectRepository struct {
	db *gorm.DB
}

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
	return repo.db.WithContext(ctx).Clauses(clause.OnConflict{UpdateAll: true}).Create(&project).Error
}

func (repo ProjectRepository) GetByName(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	var project Project
	if err := repo.db.WithContext(ctx).Where("name = ?", name).First(&project).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, store.ErrResourceNotFound
		}
		return nil, err
	}
	return project.ToTenantProject()
}

func (repo ProjectRepository) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	var projects []Project
	if err := repo.db.WithContext(ctx).Find(&projects).Error; err != nil {
		return nil, err
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
