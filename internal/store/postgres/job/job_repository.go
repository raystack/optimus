package job

import (
	"github.com/hashicorp/go-multierror"
	"golang.org/x/net/context"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobRepository struct {
	db *gorm.DB
}

func NewJobRepository(db *gorm.DB) *JobRepository {
	return &JobRepository{db: db}
}

func (j JobRepository) Add(ctx context.Context, jobs []*job.Job) (savedJobs []*job.Job, jobErrors error, err error) {
	for _, jobEntity := range jobs {
		if err := j.insertJobSpec(ctx, jobEntity); err != nil {
			jobErrors = multierror.Append(jobErrors, err)
			continue
		}
		savedJobs = append(savedJobs, jobEntity)
	}

	if len(savedJobs) == 0 {
		return nil, jobErrors, errors.NewError(errors.ErrInternalError, job.EntityJob, "no jobs to create")
	}

	return savedJobs, jobErrors, nil
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

func (j JobRepository) GetJobNameWithInternalDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Dependency, error) {
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
	j.destination AS dependency_resource_urn,
	'static' AS dependency_type
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
	j.destination AS dependency_resource_urn,
	'inferred' AS dependency_type
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

	return j.toJobNameWithDependencies(storeJobsWithDependencies)
}

func (j JobRepository) toJobNameWithDependencies(storeJobsWithDependencies []JobWithDependency) (map[job.Name][]*job.Dependency, error) {
	jobNameWithDependencies := make(map[job.Name][]*job.Dependency)

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
		jobNameWithDependencies[name] = dependencies
	}
	return jobNameWithDependencies, nil
}

func groupDependenciesPerJobFullName(dependencies []JobWithDependency) map[string][]JobWithDependency {
	dependenciesMap := make(map[string][]JobWithDependency)
	for _, dependency := range dependencies {
		dependenciesMap[dependency.getJobFullName()] = append(dependenciesMap[dependency.getJobFullName()], dependency)
	}
	return dependenciesMap
}

func (j JobRepository) toDependencies(storeDependencies []JobWithDependency) ([]*job.Dependency, error) {
	var dependencies []*job.Dependency
	for _, storeDependency := range storeDependencies {
		if storeDependency.DependencyJobName == "" {
			continue
		}
		dependencyTenant, err := tenant.NewTenant(storeDependency.DependencyProjectName, storeDependency.DependencyNamespaceName)
		if err != nil {
			return nil, err
		}
		dependency, err := job.NewDependencyResolved(storeDependency.DependencyJobName, "", storeDependency.DependencyResourceURN, dependencyTenant, storeDependency.DependencyType)
		dependencies = append(dependencies, dependency)
	}
	return dependencies, nil
}

type JobWithDependency struct {
	JobName                 string `json:"job_name"`
	ProjectName             string `json:"project_name"`
	DependencyJobName       string `json:"dependency_job_name"`
	DependencyResourceURN   string `json:"dependency_resource_urn"`
	DependencyProjectName   string `json:"dependency_project_name"`
	DependencyNamespaceName string `json:"dependency_namespace_name"`
	DependencyHost          string `json:"dependency_host"`
	DependencyType          string `json:"dependency_type"`
	DependencyState         string `json:"dependency_state"`
}

func (j JobWithDependency) getJobFullName() string {
	return j.ProjectName + "/" + j.JobName
}

func (j JobRepository) SaveDependency(ctx context.Context, jobsWithDependencies []*job.WithDependency) error {
	var storageJobDependencies []*JobWithDependency
	for _, jobWithDependencies := range jobsWithDependencies {
		dependencies, err := toJobDependency(jobWithDependencies)
		if err != nil {
			return err
		}
		storageJobDependencies = append(storageJobDependencies, dependencies...)
	}

	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := deleteDependencies(ctx, tx, storageJobDependencies); err != nil {
			return err
		}

		return insertDependencies(ctx, tx, storageJobDependencies)
	})
}

func insertDependencies(ctx context.Context, tx *gorm.DB, storageJobDependencies []*JobWithDependency) error {
	insertJobDependencyQuery := `
INSERT INTO job_dependency (
	job_name, project_name, dependency_job_name, dependency_resource_urn, 
	dependency_project_name, dependency_namespace_name, dependency_host, 
	dependency_type, dependency_state,
	created_at, updated_at
)
VALUES (
	?, ?, ?, ?,
	?, ?, ?, 
	?, ?, 
	NOW(), NOW()
);
`

	for _, dependency := range storageJobDependencies {
		result := tx.WithContext(ctx).Exec(insertJobDependencyQuery,
			dependency.JobName, dependency.ProjectName,
			dependency.DependencyJobName, dependency.DependencyResourceURN,
			dependency.DependencyProjectName, dependency.DependencyNamespaceName,
			dependency.DependencyHost, dependency.DependencyType, dependency.DependencyState)

		if result.Error != nil {
			return errors.Wrap(job.EntityJob, "unable to save job dependency", result.Error)
		}

		if result.RowsAffected == 0 {
			return errors.InternalError(job.EntityJob, "unable to save job dependency, rows affected 0", nil)
		}
	}
	return nil
}

func deleteDependencies(ctx context.Context, tx *gorm.DB, jobDependencies []*JobWithDependency) error {
	var result *gorm.DB

	var jobFullName []string
	for _, dependency := range jobDependencies {
		jobFullName = append(jobFullName, dependency.getJobFullName())
	}

	deleteForProjectScope := `DELETE
FROM job_dependency
WHERE project_name || '/' || job_name in (?);
`

	result = tx.WithContext(ctx).Exec(deleteForProjectScope, jobFullName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during delete of job dependency", result.Error)
	}

	return nil
}

func toJobDependency(jobWithDependency *job.WithDependency) ([]*JobWithDependency, error) {
	var jobDependencies []*JobWithDependency
	for _, dependency := range jobWithDependency.Dependencies() {
		var dependencyProjectName, dependencyNamespaceName string
		if dependency.Tenant().ProjectName() != "" {
			dependencyProjectName = dependency.Tenant().ProjectName().String()
		}
		namespaceName, err := dependency.Tenant().NamespaceName()
		if err != nil {
			dependencyNamespaceName = namespaceName.String()
		}

		jobDependencies = append(jobDependencies, &JobWithDependency{
			JobName:                 jobWithDependency.Name().String(),
			ProjectName:             jobWithDependency.Job().ProjectName().String(),
			DependencyJobName:       dependency.Name(),
			DependencyResourceURN:   dependency.Resource(),
			DependencyProjectName:   dependencyProjectName,
			DependencyNamespaceName: dependencyNamespaceName,
			DependencyHost:          dependency.Host(),
			DependencyType:          dependency.DependencyType().String(),
			DependencyState:         dependency.DependencyState().String(),
		})
	}
	return jobDependencies, nil
}
