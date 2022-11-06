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

	_, err = j.get(ctx, jobEntity.ProjectName(), jobEntity.Spec().Name())
	if err == nil {
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, "job already exists")
	}

	insertJobQuery := `
INSERT INTO job (
	name, version, owner, description, 
	labels, start_date, end_date, interval, 
	depends_on_past, catch_up, retry, alert, 
	static_upstreams, http_upstreams, task_name, task_config, 
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
		storageJob.StaticUpstreams, storageJob.HTTPUpstreams, storageJob.TaskName, storageJob.TaskConfig,
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

func (j JobRepository) GetJobNameWithInternalUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error) {
	query := `
WITH static_upstreams AS (
	SELECT j.name, j.project_name, d.static_upstream
	FROM job j
	JOIN UNNEST(j.static_upstreams) d(static_upstream) ON true
	WHERE project_name = ? AND
	name IN (?)
), 

inferred_upstreams AS (
	SELECT j.name, j.project_name, s.source
	FROM job j
	JOIN UNNEST(j.sources) s(source) ON true
	WHERE project_name = ? AND
	name IN (?)
)

SELECT
	su.name AS job_name, 
	su.project_name, 
	j.name AS upstream_job_name,
	j.project_name AS upstream_project_name,
	j.namespace_name AS upstream_namespace_name,
	j.destination AS upstream_resource_urn,
	'static' AS upstream_type
FROM static_upstreams su
JOIN job j ON 
	(su.static_upstream = j.name and su.project_name = j.project_name) OR 
	(su.static_upstream = j.project_name || '/' ||j.name)

UNION ALL
	
SELECT
	id.name AS job_name,
	id.project_name,
	j.name AS upstream_job_name,
	j.project_name AS upstream_project_name,
	j.namespace_name AS upstream_namespace_name,
	j.destination AS upstream_resource_urn,
	'inferred' AS upstream_type
FROM inferred_upstreams id
JOIN job j ON id.source = j.destination;
`

	jobNamesStr := make([]string, len(jobNames))
	for i, jobName := range jobNames {
		jobNamesStr[i] = jobName.String()
	}

	var storeJobsWithUpstreams []JobWithUpstream
	err := j.db.WithContext(ctx).Raw(query, projectName.String(), jobNames, projectName.String(), jobNames).
		Scan(&storeJobsWithUpstreams).Error
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with upstreams", err)
	}

	return j.toJobNameWithUpstreams(storeJobsWithUpstreams)
}

func (j JobRepository) toJobNameWithUpstreams(storeJobsWithUpstreams []JobWithUpstream) (map[job.Name][]*job.Upstream, error) {
	jobNameWithUpstreams := make(map[job.Name][]*job.Upstream)

	upstreamsPerJobName := groupUpstreamsPerJobFullName(storeJobsWithUpstreams)
	for _, storeUpstreams := range upstreamsPerJobName {
		upstreams, err := j.toUpstreams(storeUpstreams)
		if err != nil {
			return nil, err
		}
		name, err := job.NameFrom(storeUpstreams[0].JobName)
		if err != nil {
			return nil, err
		}
		jobNameWithUpstreams[name] = upstreams
	}
	return jobNameWithUpstreams, nil
}

func groupUpstreamsPerJobFullName(upstreams []JobWithUpstream) map[string][]JobWithUpstream {
	upstreamsMap := make(map[string][]JobWithUpstream)
	for _, upstream := range upstreams {
		upstreamsMap[upstream.getJobFullName()] = append(upstreamsMap[upstream.getJobFullName()], upstream)
	}
	return upstreamsMap
}

func (j JobRepository) toUpstreams(storeUpstreams []JobWithUpstream) ([]*job.Upstream, error) {
	var upstreams []*job.Upstream
	for _, storeUpstream := range storeUpstreams {
		if storeUpstream.UpstreamJobName == "" {
			continue
		}
		upstreamTenant, err := tenant.NewTenant(storeUpstream.UpstreamProjectName, storeUpstream.UpstreamNamespaceName)
		if err != nil {
			return nil, err
		}
		// TODO: consider using this error
		upstream, err := job.NewUpstreamResolved(storeUpstream.UpstreamJobName, "", storeUpstream.UpstreamResourceURN, upstreamTenant, storeUpstream.UpstreamType)
		upstreams = append(upstreams, upstream)
	}
	return upstreams, nil
}

type JobWithUpstream struct {
	JobName               string `json:"job_name"`
	ProjectName           string `json:"project_name"`
	UpstreamJobName       string `json:"upstream_job_name"`
	UpstreamResourceURN   string `json:"upstream_resource_urn"`
	UpstreamProjectName   string `json:"upstream_project_name"`
	UpstreamNamespaceName string `json:"upstream_namespace_name"`
	UpstreamHost          string `json:"upstream_host"`
	UpstreamType          string `json:"upstream_type"`
	UpstreamState         string `json:"upstream_state"`
}

func (j JobWithUpstream) getJobFullName() string {
	return j.ProjectName + "/" + j.JobName
}

func (j JobRepository) SaveUpstream(ctx context.Context, jobsWithUpstreams []*job.WithUpstream) error {
	var storageJobUpstreams []*JobWithUpstream
	for _, jobWithUpstreams := range jobsWithUpstreams {
		upstream, err := toJobUpstream(jobWithUpstreams)
		if err != nil {
			return err
		}
		storageJobUpstreams = append(storageJobUpstreams, upstream...)
	}

	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := deleteUpstreams(ctx, tx, storageJobUpstreams); err != nil {
			return err
		}

		return insertUpstreams(ctx, tx, storageJobUpstreams)
	})
}

func insertUpstreams(ctx context.Context, tx *gorm.DB, storageJobUpstreams []*JobWithUpstream) error {
	insertJobUpstreamQuery := `
INSERT INTO job_upstream (
	job_name, project_name, upstream_job_name, upstream_resource_urn, 
	upstream_project_name, upstream_namespace_name, upstream_host, 
	upstream_type, upstream_state,
	created_at, updated_at
)
VALUES (
	?, ?, ?, ?,
	?, ?, ?, 
	?, ?, 
	NOW(), NOW()
);
`

	for _, upstream := range storageJobUpstreams {
		result := tx.WithContext(ctx).Exec(insertJobUpstreamQuery,
			upstream.JobName, upstream.ProjectName,
			upstream.UpstreamJobName, upstream.UpstreamResourceURN,
			upstream.UpstreamProjectName, upstream.UpstreamNamespaceName,
			upstream.UpstreamHost, upstream.UpstreamType, upstream.UpstreamState)

		if result.Error != nil {
			return errors.Wrap(job.EntityJob, "unable to save job upstream", result.Error)
		}

		if result.RowsAffected == 0 {
			return errors.InternalError(job.EntityJob, "unable to save job upstream, rows affected 0", nil)
		}
	}
	return nil
}

func deleteUpstreams(ctx context.Context, tx *gorm.DB, jobUpstreams []*JobWithUpstream) error {
	var result *gorm.DB

	var jobFullName []string
	for _, upstream := range jobUpstreams {
		jobFullName = append(jobFullName, upstream.getJobFullName())
	}

	deleteForProjectScope := `DELETE
FROM job_upstream
WHERE project_name || '/' || job_name in (?);
`

	result = tx.WithContext(ctx).Exec(deleteForProjectScope, jobFullName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during delete of job upstream", result.Error)
	}

	return nil
}

func toJobUpstream(jobWithUpstream *job.WithUpstream) ([]*JobWithUpstream, error) {
	var jobUpstreams []*JobWithUpstream
	for _, upstream := range jobWithUpstream.Upstreams() {
		var upstreamProjectName, upstreamNamespaceName string
		// TODO: re-check this implementation as project and namespace name is not supposed to be empty within a tenant
		if upstream.Tenant().ProjectName() != "" {
			upstreamProjectName = upstream.Tenant().ProjectName().String()
		}
		if upstream.Tenant().NamespaceName() != "" {
			upstreamNamespaceName = upstream.Tenant().NamespaceName().String()
		}
		jobUpstreams = append(jobUpstreams, &JobWithUpstream{
			JobName:               jobWithUpstream.Name().String(),
			ProjectName:           jobWithUpstream.Job().ProjectName().String(),
			UpstreamJobName:       upstream.Name(),
			UpstreamResourceURN:   upstream.Resource(),
			UpstreamProjectName:   upstreamProjectName,
			UpstreamNamespaceName: upstreamNamespaceName,
			UpstreamHost:          upstream.Host(),
			UpstreamType:          upstream.Type().String(),
			UpstreamState:         upstream.State().String(),
		})
	}
	return jobUpstreams, nil
}
