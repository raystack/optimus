package job

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
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
	// TODO: Transaction per job level or jobs? -> per job
	return j.db.Transaction(func(tx *gorm.DB) error {
		for _, jobEntity := range jobs {
			if err := j.insertJobSpec(ctx, jobEntity.JobSpec()); err != nil {
				return err
			}
			if err := j.insertJobResource(ctx, jobEntity); err != nil {
				return err
			}
		}
		return nil
	})
}

func (j JobRepository) insertJobSpec(ctx context.Context, jobSpec *dto.JobSpec) error {
	spec, err := toStorageSpec(jobSpec)
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
	NOW(), NOW()
`

	result := j.db.WithContext(ctx).Exec(insertJobQuery, spec.NamespaceName, spec.ProjectName,
		spec.Name, spec.Version, spec.Owner, spec.Description, spec.Labels,
		spec.StartDate, spec.EndDate, spec.Interval, spec.DependsOnPast, spec.CatchUp,
		spec.Retry, spec.Alert, spec.StaticDependencies, spec.HTTPDependencies,
		spec.TaskName, spec.TaskConfig, spec.WindowSize, spec.WindowOffset, spec.WindowTruncateTo,
		spec.Assets, spec.Hooks, spec.Metadata,
		spec.ProjectName, spec.NamespaceName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "unable to save job spec", result.Error)
	}

	if result.RowsAffected == 0 {
		return errors.InternalError(job.EntityJob, "unable to save job spec, rows affected 0", nil)
	}
	return nil
}

func (j JobRepository) insertJobResource(ctx context.Context, jobEntity *job.Job) error {
	jobResource := NewJobResource(jobEntity.JobSpec().Name(), jobEntity.JobSpec().Tenant().Project().Name(), jobEntity.Destination(), jobEntity.Sources())

	insertJobResourceQuery := `
INSERT INTO job_resource (job_name, project_name, destination, sources, created_at, updated_at)
VALUES ?, ?, ?, ?, NOW(), NOW()
`
	result := j.db.WithContext(ctx).Exec(insertJobResourceQuery, jobResource.JobName, jobResource.ProjectName, jobResource.Destination, jobResource.Sources)
	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "unable to save job resource", result.Error)
	}

	if result.RowsAffected == 0 {
		return errors.InternalError(job.EntityJob, "unable to save job resource, rows affected 0", nil)
	}
	return nil
}

type JobResource struct {
	JobName     string
	ProjectName string

	Destination string
	Sources     []string
}

func NewJobResource(jobName job.JobName, projectName tenant.ProjectName, destination job.Destination, sources []job.Source) *JobResource {
	jobSources := make([]string, len(sources))
	for i, source := range sources {
		jobSources[i] = source.String()
	}
	return &JobResource{JobName: jobName.String(), ProjectName: projectName.String(), Destination: destination.String(), Sources: jobSources}
}
