package tenant

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type ProjectRepository struct {
	pool *pgxpool.Pool
}

const (
	projectColumns = `id, name, config, created_at, updated_at`
)

type Project struct {
	ID     uuid.UUID
	Name   string
	Config map[string]string

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
}

func (p *Project) toTenantProject() (*tenant.Project, error) {
	return tenant.NewProject(p.Name, p.Config)
}

func (repo ProjectRepository) Save(ctx context.Context, tenantProject *tenant.Project) error {
	_, err := repo.get(ctx, tenantProject.Name())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			insertProjectQuery := `INSERT INTO project (name, config, created_at, updated_at) VALUES ($1, $2, now(), now())`
			_, err = repo.pool.Exec(ctx, insertProjectQuery, tenantProject.Name(), tenantProject.GetConfigs())
			return errors.WrapIfErr(tenant.EntityProject, "unable to save project", err)
		}
		return errors.Wrap(tenant.EntityProject, "unable to save project", err)
	}

	updateProjectQuery := `UPDATE project SET config=$1, updated_at=now() WHERE name=$2`
	_, err = repo.pool.Exec(ctx, updateProjectQuery, tenantProject.GetConfigs(), tenantProject.Name())
	return errors.WrapIfErr(tenant.EntityProject, "unable to update project", err)
}

func (repo ProjectRepository) GetByName(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	project, err := repo.get(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(tenant.EntityProject, "no project for "+name.String())
		}
		return nil, errors.Wrap(tenant.EntityProject, "error while getting project", err)
	}
	return project.toTenantProject()
}

func (repo ProjectRepository) get(ctx context.Context, name tenant.ProjectName) (Project, error) {
	var project Project

	getProjectByNameQuery := `SELECT ` + projectColumns + ` FROM project WHERE name = $1 AND deleted_at IS NULL`
	err := repo.pool.QueryRow(ctx, getProjectByNameQuery, name).
		Scan(&project.ID, &project.Name, &project.Config, &project.CreatedAt, &project.UpdatedAt)
	if err != nil {
		return Project{}, err
	}
	return project, nil
}

func (repo ProjectRepository) GetAll(ctx context.Context) ([]*tenant.Project, error) {
	var projects []*tenant.Project

	getAllProjects := `SELECT ` + projectColumns + ` FROM project WHERE deleted_at IS NULL`
	rows, err := repo.pool.Query(ctx, getAllProjects)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityProject, "error in GetAll", err)
	}
	defer rows.Close()

	for rows.Next() {
		var prj Project
		err = rows.Scan(&prj.ID, &prj.Name, &prj.Config, &prj.CreatedAt, &prj.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(tenant.EntityProject, "error in GetAll", err)
		}

		project, err := prj.toTenantProject()
		if err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}

	return projects, nil
}

func NewProjectRepository(pool *pgxpool.Pool) *ProjectRepository {
	return &ProjectRepository{
		pool: pool,
	}
}
