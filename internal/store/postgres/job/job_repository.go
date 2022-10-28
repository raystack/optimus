package job

import (
	"golang.org/x/net/context"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobRepository struct {
	db *gorm.DB
}

func NewJobRepository(db *gorm.DB) *JobRepository {
	return &JobRepository{db: db}
}

func (j JobRepository) Save(ctx context.Context, jobs []*job.Job) error {
	// todo: add transaction
	for _, jobEntity := range jobs {
		if err := j.insertJobSpec(ctx, jobEntity); err != nil {
			return err
		}
	}
	return nil
}

func (j JobRepository) insertJobSpec(ctx context.Context, jobEntity *job.Job) error {
	storageJob, err := toStorageSpec(jobEntity)
	if err != nil {
		return err
	}

	_, err = j.get(ctx, jobEntity.ProjectName(), jobEntity.JobSpec().Name())
	if err == nil {
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, "job already exists")
	}

	insertJobQuery := `
INSERT INTO job (
	name, version, owner, description, 
	labels, start_date, end_date, interval, 
	depends_on_past, catch_up, retry, alert, 
	static_dependencies, http_dependencies, task_name, task_config, 
	window_size, window_offset, window_truncate_to,
	assets, hooks, metadata,
	destination, sources, 
	project_name, namespace_name,
	created_at, updated_at
)
VALUES (
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, ?, 
	?, ?, ?, 
	?, ?, ?, 
	?, ?,
	?, ?,
	NOW(), NOW()
);
`

	result := j.db.WithContext(ctx).Exec(insertJobQuery,
		storageJob.Name, storageJob.Version, storageJob.Owner, storageJob.Description,
		storageJob.Labels, storageJob.StartDate, storageJob.EndDate, storageJob.Interval,
		storageJob.DependsOnPast, storageJob.CatchUp, storageJob.Retry, storageJob.Alert,
		storageJob.StaticDependencies, storageJob.HTTPDependencies, storageJob.TaskName, storageJob.TaskConfig,
		storageJob.WindowSize, storageJob.WindowOffset, storageJob.WindowTruncateTo,
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

func (j JobRepository) get(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (Spec, error) {
	var spec Spec

	getJobByNameAtProject := `SELECT name
FROM job
WHERE name = ?
AND project_name = ?
`
	err := j.db.WithContext(ctx).Raw(getJobByNameAtProject, jobName.String(), projectName.String()).
		First(&spec).Error

	return spec, err
}

func (j JobRepository) GetJobWithDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) ([]*job.WithDependency, error) {
	query := `
WITH static_dependencies AS (
	SELECT j.name, j.project_name, d.static_dependency
	FROM job j
	JOIN UNNEST(j.static_dependencies) d(static_dependency) ON true
	WHERE project_name = ? AND
	name IN (?)
), 

inferred_dependencies AS (
	SELECT j.name, j.project_name, s.source
	FROM job j
	JOIN UNNEST(j.sources) s(source) ON true
	WHERE project_name = ? AND
	name IN (?)
)

SELECT
	sd.name AS job_name, 
	sd.project_name, 
	j.name AS dependency_job_name,
	j.project_name AS dependency_project_name,
	j.namespace_name AS dependency_namespace_name,
	j.destination AS dependency_resource
FROM static_dependencies sd
JOIN job j ON 
	(sd.static_dependency = j.name and sd.project_name = j.project_name) OR 
	(sd.static_dependency = j.project_name || '/' ||j.name)

UNION ALL
	
SELECT
	id.name AS job_name,
	id.project_name,
	j.name AS dependency_job_name,
	j.project_name AS dependency_project_name,
	j.namespace_name AS dependency_namespace_name,
	j.destination AS dependency_resource
FROM inferred_dependencies id
JOIN job j ON id.source = j.destination;
`

	jobNamesStr := make([]string, len(jobNames))
	for i, jobName := range jobNames {
		jobNamesStr[i] = jobName.String()
	}

	var storeJobsWithDependencies []JobWithDependency
	err := j.db.WithContext(ctx).Raw(query, projectName.String(), jobNames, projectName.String(), jobNames).
		Scan(&storeJobsWithDependencies).Error
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with dependencies", err)
	}

	return j.toJobWithDependencies(storeJobsWithDependencies)
}

func (j JobRepository) toJobWithDependencies(storeJobsWithDependencies []JobWithDependency) ([]*job.WithDependency, error) {
	var jobWithDependencies []*job.WithDependency

	dependenciesPerJobName := groupDependenciesPerJobFullName(storeJobsWithDependencies)
	for _, storeDependencies := range dependenciesPerJobName {
		dependencies, err := j.toDependencies(storeDependencies)
		if err != nil {
			return nil, err
		}
		name, err := job.NameFrom(storeDependencies[0].JobName)
		if err != nil {
			return nil, err
		}
		projectName, err := tenant.ProjectNameFrom(storeDependencies[0].ProjectName)
		if err != nil {
			return nil, err
		}
		jobWithDependencies = append(jobWithDependencies, job.NewWithDependency(name, projectName, dependencies, nil))
	}
	return jobWithDependencies, nil
}

func groupDependenciesPerJobFullName(dependencies []JobWithDependency) map[string][]JobWithDependency {
	dependenciesMap := make(map[string][]JobWithDependency)
	for _, dependency := range dependencies {
		dependenciesMap[dependency.getJobFullName()] = append(dependenciesMap[dependency.getJobFullName()], dependency)
	}
	return dependenciesMap
}

func (j JobRepository) toDependencies(storeDependencies []JobWithDependency) ([]*dto.Dependency, error) {
	var dependencies []*dto.Dependency
	for _, storeDependency := range storeDependencies {
		if storeDependency.DependencyJobName == "" {
			continue
		}
		dependencyTenant, err := tenant.NewTenant(storeDependency.DependencyProjectName, storeDependency.DependencyNamespaceName)
		if err != nil {
			return nil, err
		}
		dependency := dto.NewDependency(storeDependency.DependencyJobName, dependencyTenant, "", storeDependency.DependencyResource)
		dependencies = append(dependencies, dependency)
	}
	return dependencies, nil
}
