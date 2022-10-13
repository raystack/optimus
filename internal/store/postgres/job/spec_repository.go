package job

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/internal/errors"
	"golang.org/x/net/context"
	"gorm.io/gorm"
)

type SpecRepository struct {
	db *gorm.DB
}

func NewSpecRepository(db *gorm.DB) *SpecRepository {
	return &SpecRepository{db: db}
}

func (j SpecRepository) Save(ctx context.Context, jobSpec *dto.JobSpec) error {
	spec, err := toStorageSpec(jobSpec)
	if err != nil {
		return err
	}

	insertJob := `
INSERT INTO job (
	name, version, owner, description, 
	labels, start_date, end_date, interval, 
	depends_on_past, catch_up, retry, alert, 
	static_dependencies, http_dependencies, task_name, task_config, 
	window_size, windows_offset, window_truncate_to,
	assets, hooks, metadata, 
	project_name, namespace_name,
	created_at, updated_at
)
SELECT 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, 
	?, ?, ?, 
	?, ?,
	NOW(), NOW()
FROM cte_tenant c
`

	result := j.db.WithContext(ctx).Exec(insertJob, spec.NamespaceName, spec.ProjectName,
		spec.Name, spec.Version, spec.Owner, spec.Description, spec.Labels,
		spec.StartDate, spec.EndDate, spec.Interval, spec.DependsOnPast, spec.CatchUp,
		spec.Retry, spec.Alert, spec.StaticDependencies, spec.HTTPDependencies,
		spec.TaskName, spec.TaskConfig, spec.WindowSize, spec.WindowOffset, spec.WindowTruncateTo,
		spec.Assets, spec.Hooks, spec.Metadata,
		spec.ProjectName, spec.NamespaceName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "unable to save job spec", err)
	}

	if result.RowsAffected == 0 {
		return errors.InternalError(job.EntityJob, "unable to save, rows affected 0", nil)
	}

	return nil
}
