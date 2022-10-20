package job

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/internal/errors"
	"golang.org/x/net/context"
	"gorm.io/gorm"
)

type JobRepository struct {
	db *gorm.DB
}

func NewJobRepository(db *gorm.DB) *JobRepository {
	return &JobRepository{db: db}
}

func (j JobRepository) Save(ctx context.Context, jobs []*job.Job) error {
	for _, jobEntity := range jobs {
		if err := j.insertJobSpec(ctx, jobEntity); err != nil {
			return err
		}
	}
	return nil
}

func (j JobRepository) GetJobWithDependencies(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error) {
	//TODO implement me
	panic("implement me")
}

func (j JobRepository) insertJobSpec(ctx context.Context, jobEntity *job.Job) error {
	storageJob, err := toStorageSpec(jobEntity)
	if err != nil {
		return err
	}

	insertJobQuery := `
INSERT INTO job (
	name, version, owner, description, 
	labels, start_date, end_date, interval, 
	depends_on_past, catch_up, retry, alert, 
	static_dependencies, http_dependencies, task_name, task_config, 
	window_size, windows_offset, window_truncate_to,
	assets, hooks, metadata,
	destination, sources, 
	project_name, namespace_name,
	created_at, updated_at
)
VALUES
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, 
	?, ?, ?, 
	?, ?,
	?, ?,
	NOW(), NOW()
`

	result := j.db.WithContext(ctx).Exec(insertJobQuery, storageJob.NamespaceName, storageJob.ProjectName,
		storageJob.Name, storageJob.Version, storageJob.Owner, storageJob.Description, storageJob.Labels,
		storageJob.StartDate, storageJob.EndDate, storageJob.Interval, storageJob.DependsOnPast, storageJob.CatchUp,
		storageJob.Retry, storageJob.Alert, storageJob.StaticDependencies, storageJob.HTTPDependencies,
		storageJob.TaskName, storageJob.TaskConfig, storageJob.WindowSize, storageJob.WindowOffset, storageJob.WindowTruncateTo,
		storageJob.Assets, storageJob.Hooks, storageJob.Metadata,
		storageJob.Destination, storageJob.Sources,
		storageJob.ProjectName, storageJob.NamespaceName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "unable to save job spec", result.Error)
	}

	if result.RowsAffected == 0 {
		return errors.InternalError(job.EntityJob, "unable to save job spec, rows affected 0", nil)
	}
	return nil
}
