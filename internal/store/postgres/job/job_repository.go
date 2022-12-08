package job

import (
	"context"
	"fmt"

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

func (j JobRepository) Add(ctx context.Context, jobs []*job.Job) ([]*job.Job, error) {
	me := errors.NewMultiError("add jobs errors")
	var storedJobs []*job.Job
	for _, jobEntity := range jobs {
		if err := j.insertJobSpec(ctx, jobEntity); err != nil {
			me.Append(err)
			continue
		}
		storedJobs = append(storedJobs, jobEntity)
	}
	return storedJobs, errors.MultiToError(me)
}

func (j JobRepository) insertJobSpec(ctx context.Context, jobEntity *job.Job) error {
	existingJob, err := j.get(ctx, jobEntity.ProjectName(), jobEntity.Spec().Name(), false)
	if err != nil && !errors.IsInType(err, errors.ErrNotFound) {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("failed to check job %s in db: %s", jobEntity.Spec().Name().String(), err.Error()))
	}
	if err == nil && !existingJob.DeletedAt.Valid {
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, fmt.Sprintf("job %s already exists in namespace %s", existingJob.Name, existingJob.NamespaceName))
	}
	if err == nil && existingJob.DeletedAt.Valid && existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() {
		errorMsg := fmt.Sprintf("job %s already exists and soft deleted in namespace %s. consider hard delete the job before inserting to this namespace.", existingJob.Name, existingJob.NamespaceName)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	if err == nil && existingJob.DeletedAt.Valid && existingJob.NamespaceName == jobEntity.Tenant().NamespaceName().String() {
		return j.triggerUpdate(ctx, jobEntity)
	}
	return j.triggerInsert(ctx, jobEntity)
}

func (j JobRepository) triggerInsert(ctx context.Context, jobEntity *job.Job) error {
	storageJob, err := toStorageSpec(jobEntity)
	if err != nil {
		return err
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

func (j JobRepository) Update(ctx context.Context, jobs []*job.Job) ([]*job.Job, error) {
	me := errors.NewMultiError("update jobs errors")
	var storedJobs []*job.Job
	for _, jobEntity := range jobs {
		if err := j.preCheckUpdate(ctx, jobEntity); err != nil {
			me.Append(err)
			continue
		}
		if err := j.triggerUpdate(ctx, jobEntity); err != nil {
			me.Append(err)
			continue
		}
		storedJobs = append(storedJobs, jobEntity)
	}
	return storedJobs, errors.MultiToError(me)
}

func (j JobRepository) preCheckUpdate(ctx context.Context, jobEntity *job.Job) error {
	existingJob, err := j.get(ctx, jobEntity.ProjectName(), jobEntity.Spec().Name(), false)
	if err != nil && errors.IsInType(err, errors.ErrNotFound) {
		return errors.NewError(errors.ErrNotFound, job.EntityJob, fmt.Sprintf("job %s not exists yet", jobEntity.Spec().Name()))
	}
	if err != nil {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("failed to check job %s in db: %s", jobEntity.Spec().Name().String(), err.Error()))
	}
	if existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() && existingJob.DeletedAt.Valid {
		errorMsg := fmt.Sprintf("job %s already exists and soft deleted in namespace %s.", existingJob.Name, existingJob.NamespaceName)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	if existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() && !existingJob.DeletedAt.Valid {
		errorMsg := fmt.Sprintf("job %s already exists in namespace %s.", existingJob.Name, existingJob.NamespaceName)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	if existingJob.DeletedAt.Valid {
		errorMsg := fmt.Sprintf("update is not allowed as job %s has been soft deleted. please re-add the job before updating.", existingJob.Name)
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
	}
	return nil
}

func (j JobRepository) triggerUpdate(ctx context.Context, jobEntity *job.Job) error {
	storageJob, err := toStorageSpec(jobEntity)
	if err != nil {
		return err
	}

	updateJobQuery := `
UPDATE job SET 
	version = ?, owner = ?, description = ?, 
	labels = ?, start_date = ?, end_date = ?, interval = ?,
	depends_on_past = ?, catch_up = ?, retry = ?, alert = ?,
	static_upstreams = ?, http_upstreams = ?, task_name = ?, task_config = ?,
	window_size = ?, window_offset = ?, window_truncate_to = ?,
	assets = ?, hooks = ?, metadata = ?,
	destination = ?, sources = ?,
	updated_at = NOW(), deleted_at = null
WHERE 
	name = ? AND 
	project_name = ?;
`

	result := j.db.WithContext(ctx).Exec(updateJobQuery,
		storageJob.Version, storageJob.Owner, storageJob.Description,
		storageJob.Labels, storageJob.StartDate, storageJob.EndDate, storageJob.Interval,
		storageJob.DependsOnPast, storageJob.CatchUp, storageJob.Retry, storageJob.Alert,
		storageJob.StaticUpstreams, storageJob.HTTPUpstreams, storageJob.TaskName, storageJob.TaskConfig,
		storageJob.WindowSize, storageJob.WindowOffset, storageJob.WindowTruncateTo,
		storageJob.Assets, storageJob.Hooks, storageJob.Metadata,
		storageJob.Destination, storageJob.Sources,
		storageJob.Name, storageJob.ProjectName)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "unable to update job spec", result.Error)
	}

	if result.RowsAffected == 0 {
		return errors.InternalError(job.EntityJob, "unable to update job spec, rows affected 0", nil)
	}
	return nil
}

func (j JobRepository) get(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, onlyActiveJob bool) (*Spec, error) {
	var spec Spec

	getJobByNameAtProject := `SELECT *
FROM job
WHERE name = ?
AND project_name = ?
`
	if onlyActiveJob {
		jobDeletedFilter := " AND deleted_at IS NULL"
		getJobByNameAtProject += jobDeletedFilter
	}

	err := j.db.WithContext(ctx).Raw(getJobByNameAtProject, jobName.String(), projectName.String()).
		First(&spec).Error
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errors.NotFound(job.EntityJob, fmt.Sprintf("job %s not found in project %s", jobName.String(), projectName.String()))
	}
	return &spec, err
}

func (j JobRepository) ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error) {
	query := `
WITH static_upstreams AS (
	SELECT j.name, j.project_name, d.static_upstream
	FROM job j
	JOIN UNNEST(j.static_upstreams) d(static_upstream) ON true
	WHERE project_name = ? 
    AND name IN (?) 
	AND j.deleted_at IS NULL
), 

inferred_upstreams AS (
	SELECT j.name, j.project_name, s.source
	FROM job j
	JOIN UNNEST(j.sources) s(source) ON true
	WHERE project_name = ?
	AND name IN (?)
	AND j.deleted_at IS NULL
)

SELECT
	su.name AS job_name, 
	su.project_name, 
	j.name AS upstream_job_name,
	j.project_name AS upstream_project_name,
	j.namespace_name AS upstream_namespace_name,
	j.destination AS upstream_resource_urn,
	j.task_name AS upstream_task_name,
	'static' AS upstream_type,
	false AS upstream_external
FROM static_upstreams su
JOIN job j ON 
	(su.static_upstream = j.name and su.project_name = j.project_name) OR 
	(su.static_upstream = j.project_name || '/' ||j.name)
WHERE j.deleted_at IS NULL

UNION ALL
	
SELECT
	id.name AS job_name,
	id.project_name,
	j.name AS upstream_job_name,
	j.project_name AS upstream_project_name,
	j.namespace_name AS upstream_namespace_name,
	j.destination AS upstream_resource_urn,
	j.task_name AS upstream_task_name,
	'inferred' AS upstream_type,
	false AS upstream_external
FROM inferred_upstreams id
JOIN job j ON id.source = j.destination 
WHERE j.deleted_at IS NULL;
`

	jobNamesStr := make([]string, len(jobNames))
	for i, jobName := range jobNames {
		jobNamesStr[i] = jobName.String()
	}

	var storeJobsWithUpstreams []JobWithUpstream
	if err := j.db.WithContext(ctx).Raw(query, projectName.String(), jobNamesStr, projectName.String(), jobNames).
		Scan(&storeJobsWithUpstreams).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with upstreams", err)
	}

	return j.toJobNameWithUpstreams(storeJobsWithUpstreams)
}

func (j JobRepository) toJobNameWithUpstreams(storeJobsWithUpstreams []JobWithUpstream) (map[job.Name][]*job.Upstream, error) {
	me := errors.NewMultiError("to job name with upstreams errors")
	upstreamsPerJobName := groupUpstreamsPerJobFullName(storeJobsWithUpstreams)

	jobNameWithUpstreams := make(map[job.Name][]*job.Upstream)
	for _, storeUpstreams := range upstreamsPerJobName {
		if len(storeUpstreams) == 0 {
			continue
		}
		upstreams, err := j.toUpstreams(storeUpstreams)
		if err != nil {
			me.Append(err)
			continue
		}
		name, err := job.NameFrom(storeUpstreams[0].JobName)
		if err != nil {
			me.Append(err)
			continue
		}
		jobNameWithUpstreams[name] = upstreams
	}

	if err := errors.MultiToError(me); err != nil {
		return nil, err
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

func (JobRepository) toUpstreams(storeUpstreams []JobWithUpstream) ([]*job.Upstream, error) {
	me := errors.NewMultiError("to upstreams errors")

	var upstreams []*job.Upstream
	for _, storeUpstream := range storeUpstreams {
		resourceURN := job.ResourceURN(storeUpstream.UpstreamResourceURN)
		upstreamName, _ := job.NameFrom(storeUpstream.UpstreamJobName)
		projectName, _ := tenant.ProjectNameFrom(storeUpstream.UpstreamProjectName)

		if storeUpstream.UpstreamState == job.UpstreamStateUnresolved.String() && storeUpstream.UpstreamJobName != "" {
			upstreams = append(upstreams, job.NewUpstreamUnresolvedStatic(upstreamName, projectName))
			continue
		}

		if storeUpstream.UpstreamState == job.UpstreamStateUnresolved.String() && storeUpstream.UpstreamResourceURN != "" {
			upstreams = append(upstreams, job.NewUpstreamUnresolvedInferred(resourceURN))
			continue
		}

		upstreamTenant, err := tenant.NewTenant(storeUpstream.UpstreamProjectName, storeUpstream.UpstreamNamespaceName)
		if err != nil {
			me.Append(err)
			continue
		}
		taskName, err := job.TaskNameFrom(storeUpstream.UpstreamTaskName)
		if err != nil {
			me.Append(err)
			continue
		}

		upstreamType, err := job.UpstreamTypeFrom(storeUpstream.UpstreamType)
		if err != nil {
			continue
		}
		upstream := job.NewUpstreamResolved(upstreamName, storeUpstream.UpstreamHost, resourceURN, upstreamTenant, upstreamType, taskName, storeUpstream.UpstreamExternal)
		upstreams = append(upstreams, upstream)
	}
	if err := errors.MultiToError(me); err != nil {
		return nil, err
	}
	return upstreams, nil
}

func (j JobRepository) GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error) {
	spec, err := j.get(ctx, projectName, jobName, true)
	if err != nil {
		return nil, err
	}

	job, err := specToJob(spec)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (j JobRepository) GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error) {
	specs := []Spec{}
	me := errors.NewMultiError("get all job specs by project name errors")

	getAllByProjectName := `SELECT *
FROM job
WHERE project_name = ? 
AND deleted_at IS NULL;
`
	if err := j.db.WithContext(ctx).Raw(getAllByProjectName, projectName).Find(&specs).Error; err != nil {
		return nil, err
	}

	jobs := []*job.Job{}
	for _, spec := range specs {
		job, err := specToJob(&spec)
		if err != nil {
			me.Append(err)
			continue
		}
		jobs = append(jobs, job)
	}
	if len(me.Errors) > 0 {
		return jobs, me
	}

	return jobs, nil
}

func (j JobRepository) GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error) {
	specs := []Spec{}
	me := errors.NewMultiError("get all job specs by resource destination")

	getAllByProjectName := `SELECT *
FROM job
WHERE destination = ? 
AND deleted_at IS NULL;
`
	if err := j.db.WithContext(ctx).Raw(getAllByProjectName, resourceDestination).Find(&specs).Error; err != nil {
		return nil, err
	}

	jobs := []*job.Job{}
	for _, spec := range specs {
		job, err := specToJob(&spec)
		if err != nil {
			me.Append(err)
			continue
		}
		jobs = append(jobs, job)
	}
	if len(me.Errors) > 0 {
		return jobs, me
	}

	return jobs, nil
}

func specToJob(spec *Spec) (*job.Job, error) {
	jobSpec, err := fromStorageSpec(spec)
	if err != nil {
		return nil, err
	}

	tenantName, err := tenant.NewTenant(spec.ProjectName, spec.NamespaceName)
	if err != nil {
		return nil, err
	}

	destination := job.ResourceURN(spec.Destination)

	sources := []job.ResourceURN{}
	for _, source := range spec.Sources {
		resourceURN := job.ResourceURN(source)
		sources = append(sources, resourceURN)
	}

	return job.NewJob(tenantName, jobSpec, destination, sources), nil
}

type JobWithUpstream struct {
	JobName               string `json:"job_name"`
	ProjectName           string `json:"project_name"`
	UpstreamJobName       string `json:"upstream_job_name"`
	UpstreamResourceURN   string `json:"upstream_resource_urn"`
	UpstreamProjectName   string `json:"upstream_project_name"`
	UpstreamNamespaceName string `json:"upstream_namespace_name"`
	UpstreamTaskName      string `json:"upstream_task_name"`
	UpstreamHost          string `json:"upstream_host"`
	UpstreamType          string `json:"upstream_type"`
	UpstreamState         string `json:"upstream_state"`
	UpstreamExternal      bool   `json:"upstream_external"`
}

func (j JobWithUpstream) getJobFullName() string {
	return j.ProjectName + "/" + j.JobName
}

func (j JobRepository) ReplaceUpstreams(ctx context.Context, jobsWithUpstreams []*job.WithUpstream) error {
	var storageJobUpstreams []*JobWithUpstream
	for _, jobWithUpstreams := range jobsWithUpstreams {
		upstream := toJobUpstream(jobWithUpstreams)
		storageJobUpstreams = append(storageJobUpstreams, upstream...)
	}

	// TODO: check how to optimize lock: row level not table level lock
	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var jobFullName []string
		for _, upstream := range storageJobUpstreams {
			jobFullName = append(jobFullName, upstream.getJobFullName())
		}

		if err := j.deleteUpstreams(tx, jobFullName); err != nil {
			return err
		}
		return j.insertUpstreams(tx, storageJobUpstreams)
	})
}

func (JobRepository) insertUpstreams(tx *gorm.DB, storageJobUpstreams []*JobWithUpstream) error {
	insertResolvedUpstreamQuery := `
INSERT INTO job_upstream (
	job_id, job_name, project_name, 
	upstream_job_id, upstream_job_name, upstream_resource_urn, 
	upstream_project_name, upstream_namespace_name, upstream_host,
	upstream_task_name, upstream_external,
	upstream_type, upstream_state,
	created_at
)
VALUES (
	(select id FROM job WHERE name = ?), ?, ?, 
	(select id FROM job WHERE name = ?), ?, ?,
	?, ?, ?, 
	?, ?,
	?, ?, 
	NOW()
);
`

	insertUnresolvedUpstreamQuery := `
INSERT INTO job_upstream (
	job_id, job_name, project_name, 
	upstream_job_name, upstream_resource_urn, upstream_project_name,
	upstream_type, upstream_state,
	created_at
)
VALUES (
	(select id FROM job WHERE name = ?), ?, ?, 
	?, ?, ?,
	?, ?, 
	NOW()
);
`

	for _, upstream := range storageJobUpstreams {
		var result *gorm.DB
		if upstream.UpstreamState == job.UpstreamStateResolved.String() {
			result = tx.Exec(insertResolvedUpstreamQuery,
				upstream.JobName, upstream.JobName, upstream.ProjectName,
				upstream.UpstreamJobName, upstream.UpstreamJobName, upstream.UpstreamResourceURN,
				upstream.UpstreamProjectName, upstream.UpstreamNamespaceName, upstream.UpstreamHost,
				upstream.UpstreamTaskName, upstream.UpstreamExternal,
				upstream.UpstreamType, upstream.UpstreamState)
		} else {
			result = tx.Exec(insertUnresolvedUpstreamQuery,
				upstream.JobName, upstream.JobName, upstream.ProjectName,
				upstream.UpstreamJobName, upstream.UpstreamResourceURN, upstream.UpstreamProjectName,
				upstream.UpstreamType, upstream.UpstreamState)
		}

		if result.Error != nil {
			return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("unable to save job upstream: %s", result.Error))
		}

		if result.RowsAffected == 0 {
			return errors.NewError(errors.ErrInternalError, job.EntityJob, "unable to save job upstream, rows affected 0")
		}
	}
	return nil
}

func (JobRepository) deleteUpstreams(tx *gorm.DB, jobUpstreams []string) error {
	var result *gorm.DB

	deleteForProjectScope := `DELETE
FROM job_upstream
WHERE project_name || '/' || job_name in (?);
`

	result = tx.Exec(deleteForProjectScope, jobUpstreams)

	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during delete of job upstream", result.Error)
	}

	return nil
}

func toJobUpstream(jobWithUpstream *job.WithUpstream) []*JobWithUpstream {
	var jobUpstreams []*JobWithUpstream
	for _, upstream := range jobWithUpstream.Upstreams() {
		var upstreamProjectName, upstreamNamespaceName string
		// TODO: re-check this implementation as project and namespace name is not supposed to be empty within a tenant
		if upstream.ProjectName() != "" {
			upstreamProjectName = upstream.ProjectName().String()
		}
		if upstream.NamespaceName() != "" {
			upstreamNamespaceName = upstream.NamespaceName().String()
		}
		jobUpstreams = append(jobUpstreams, &JobWithUpstream{
			JobName:               jobWithUpstream.Name().String(),
			ProjectName:           jobWithUpstream.Job().ProjectName().String(),
			UpstreamJobName:       upstream.Name().String(),
			UpstreamResourceURN:   upstream.Resource().String(),
			UpstreamProjectName:   upstreamProjectName,
			UpstreamNamespaceName: upstreamNamespaceName,
			UpstreamTaskName:      upstream.TaskName().String(),
			UpstreamHost:          upstream.Host(),
			UpstreamType:          upstream.Type().String(),
			UpstreamState:         upstream.State().String(),
			UpstreamExternal:      upstream.External(),
		})
	}
	return jobUpstreams
}

type ProjectAndJobNames struct {
	ProjectName string `json:"project_name"`
	JobName     string `json:"job_name"`
}

func (j JobRepository) Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error {
	if cleanHistory {
		return j.hardDelete(ctx, projectName, jobName)
	}
	return j.softDelete(ctx, projectName, jobName)
}

func (j JobRepository) hardDelete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) error {
	query := `
DELETE 
FROM job
WHERE project_name = ? AND name = ?
`
	result := j.db.WithContext(ctx).Exec(query, projectName.String(), jobName.String())
	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during job deletion", result.Error)
	}
	if result.RowsAffected == 0 {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("job %s failed to be deleted", jobName.String()))
	}
	return nil
}

func (j JobRepository) softDelete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) error {
	query := `
UPDATE job
SET deleted_at = current_timestamp
WHERE project_name = ? AND name = ?
`
	result := j.db.WithContext(ctx).Exec(query, projectName.String(), jobName.String())
	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during job deletion", result.Error)
	}
	if result.RowsAffected == 0 {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("job %s failed to be deleted", jobName.String()))
	}
	return nil
}

func (j JobRepository) GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error) {
	var specs []Spec
	me := errors.NewMultiError("get all job specs by project name errors")

	getAllByProjectName := `SELECT *
FROM job
WHERE project_name = ? 
AND namespace_name = ? 
AND deleted_at IS NULL;
`
	if err := j.db.WithContext(ctx).Raw(getAllByProjectName, jobTenant.ProjectName().String(), jobTenant.NamespaceName().String()).Find(&specs).Error; err != nil {
		return nil, err
	}

	var jobs []*job.Job
	for _, spec := range specs {
		jobSpec, err := fromStorageSpec(&spec)
		if err != nil {
			me.Append(err)
			continue
		}
		// TODO: pass destination and sources values
		job := job.NewJob(jobTenant, jobSpec, "", nil)
		jobs = append(jobs, job)
	}

	return jobs, errors.MultiToError(me)
}

func (j JobRepository) GetUpstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Upstream, error) {
	query := `
SELECT
	*
FROM job_upstream
WHERE project_name=? AND job_name=?;
`

	var storeJobsWithUpstreams []JobWithUpstream
	if err := j.db.WithContext(ctx).Raw(query, projectName.String(), jobName.String()).
		Scan(&storeJobsWithUpstreams).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with upstreams", err)
	}

	return j.toUpstreams(storeJobsWithUpstreams)
}

func (j JobRepository) GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination job.ResourceURN) ([]*job.Downstream, error) {
	query := `
SELECT
	name as job_name, project_name, namespace_name, task_name
FROM job
WHERE project_name = ? AND ? = ANY(sources)
AND deleted_at IS NULL;
`

	var storeDownstream []Downstream
	if err := j.db.WithContext(ctx).Raw(query, projectName.String(), destination.String()).
		Scan(&storeDownstream).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting downstream by destination", err)
	}

	return fromStoreDownstream(storeDownstream)
}

type Downstream struct {
	JobName       string `json:"job_name"`
	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`
	TaskName      string `json:"task_name"`
}

func (j JobRepository) GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Downstream, error) {
	query := `
SELECT
	j.name as job_name, j.project_name, j.namespace_name, j.task_name
FROM job_upstream ju
JOIN job j ON (ju.job_name = j.name AND ju.project_name = j.project_name)
WHERE upstream_project_name=? AND upstream_job_name=?
AND j.deleted_at IS NULL;
`

	var storeDownstream []Downstream
	if err := j.db.WithContext(ctx).Raw(query, projectName.String(), jobName.String()).
		Scan(&storeDownstream).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting downstream by job name", err)
	}

	return fromStoreDownstream(storeDownstream)
}

func fromStoreDownstream(storeDownstreamList []Downstream) ([]*job.Downstream, error) {
	var downstreamList []*job.Downstream
	me := errors.NewMultiError("get downstream by destination errors")
	for _, storeDownstream := range storeDownstreamList {
		downstreamJobName, err := job.NameFrom(storeDownstream.JobName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamProjectName, err := tenant.ProjectNameFrom(storeDownstream.ProjectName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamNamespaceName, err := tenant.NamespaceNameFrom(storeDownstream.NamespaceName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamTaskName, err := job.TaskNameFrom(storeDownstream.TaskName)
		if err != nil {
			me.Append(err)
			continue
		}
		downstreamList = append(downstreamList, job.NewDownstream(downstreamJobName, downstreamProjectName, downstreamNamespaceName, downstreamTaskName))
	}
	return downstreamList, errors.MultiToError(me)
}
