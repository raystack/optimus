package job

import (
	"context"
	"fmt"

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
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("failed to check job %s in db: %s", jobEntity.Spec().Name().String(), err.Error()))
	} else if err == nil {
		if existingJob.DeletedAt.Valid {
			if existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() {
				errorMsg := fmt.Sprintf("job already exists and soft deleted in namespace %s. consider hard delete the job before inserting in this namespace.", existingJob.NamespaceName)
				return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
			}
			return j.triggerUpdate(ctx, jobEntity)
		}
		return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, "job already exists")
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
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.NewError(errors.ErrNotFound, job.EntityJob, fmt.Sprintf("job %s not exists yet", jobEntity.Spec().Name()))
		}
		return errors.NewError(errors.ErrInternalError, job.EntityJob, fmt.Sprintf("failed to check job %s in db: %s", jobEntity.Spec().Name().String(), err.Error()))
	}

	if existingJob.DeletedAt.Valid {
		if existingJob.NamespaceName != jobEntity.Tenant().NamespaceName().String() {
			errorMsg := fmt.Sprintf("job already exists and soft deleted in namespace %s. consider hard delete the job and do add to this namespace.", existingJob.NamespaceName)
			return errors.NewError(errors.ErrAlreadyExists, job.EntityJob, errorMsg)
		}
		errorMsg := fmt.Sprintf("update is not allowed as job %s has been soft deleted. please do add operation.", existingJob.Name)
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
		jobDeletedFilter := " AND deleted_at IS NOT NULL"
		getJobByNameAtProject += jobDeletedFilter
	}

	err := j.db.WithContext(ctx).Raw(getJobByNameAtProject, jobName.String(), projectName.String()).
		First(&spec).Error

	return &spec, err
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
	j.task_name AS upstream_task_name,
	'static' AS upstream_type,
	false AS upstream_external
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
	j.task_name AS upstream_task_name,
	'inferred' AS upstream_type,
	false AS upstream_external
FROM inferred_upstreams id
JOIN job j ON id.source = j.destination;
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

		if storeUpstream.UpstreamState == job.UpstreamStateUnresolved.String() {
			upstreams = append(upstreams, job.NewUpstreamUnresolved(upstreamName, resourceURN, projectName))
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

		upstream, err := job.NewUpstreamResolved(upstreamName, storeUpstream.UpstreamHost, resourceURN, upstreamTenant, storeUpstream.UpstreamType, taskName, storeUpstream.UpstreamExternal)
		if err != nil {
			me.Append(err)
			continue
		}
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
	me := errors.NewMultiError("convert orm spec to job entity")
	jobSpec, err := fromStorageSpec(spec)
	if err != nil {
		return nil, err
	}

	tenantName, err := tenant.NewTenant(spec.ProjectName, spec.NamespaceName)
	me.Append(err)

	destination := job.ResourceURN(spec.Destination)
	me.Append(err)

	sources := []job.ResourceURN{}
	for _, source := range spec.Sources {
		resourceURN := job.ResourceURN(source)
		me.Append(err)
		sources = append(sources, resourceURN)
	}

	if len(me.Errors) > 0 {
		return nil, me
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
	insertJobUpstreamQuery := `
INSERT INTO job_upstream (
	job_name, project_name, upstream_job_name, upstream_resource_urn, 
	upstream_project_name, upstream_namespace_name, upstream_host,
	upstream_task_name, upstream_external,
	upstream_type, upstream_state,
	created_at, updated_at
)
VALUES (
	?, ?, ?, ?,
	?, ?, ?, 
	?, ?,
	?, ?, 
	NOW(), NOW()
);
`

	for _, upstream := range storageJobUpstreams {
		result := tx.Exec(insertJobUpstreamQuery,
			upstream.JobName, upstream.ProjectName,
			upstream.UpstreamJobName, upstream.UpstreamResourceURN,
			upstream.UpstreamProjectName, upstream.UpstreamNamespaceName, upstream.UpstreamHost,
			upstream.UpstreamTaskName, upstream.UpstreamExternal,
			upstream.UpstreamType, upstream.UpstreamState)

		if result.Error != nil {
			return errors.Wrap(job.EntityJob, "unable to save job upstream", result.Error)
		}

		if result.RowsAffected == 0 {
			return errors.InternalError(job.EntityJob, "unable to save job upstream, rows affected 0", nil)
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

func (j JobRepository) GetDownstreamFullNames(ctx context.Context, subjectProjectName tenant.ProjectName, subjectJobName job.Name) ([]job.FullName, error) {
	query := `
SELECT 
project_name, job_name
FROM job_upstream
WHERE upstream_project_name = ? AND upstream_job_name = ?
AND upstream_state = 'resolved'
`

	var projectAndJobNames []ProjectAndJobNames
	if err := j.db.WithContext(ctx).Raw(query, subjectProjectName.String(), subjectJobName.String()).
		Scan(&projectAndJobNames).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job downstream", err)
	}

	var fullNames []job.FullName
	for _, projectAndJobName := range projectAndJobNames {
		projectName, err := tenant.ProjectNameFrom(projectAndJobName.ProjectName)
		if err != nil {
			return nil, errors.Wrap(job.EntityJob, "error while getting job downstream", err)
		}
		jobName, err := job.NameFrom(projectAndJobName.JobName)
		if err != nil {
			return nil, errors.Wrap(job.EntityJob, "error while getting job downstream", err)
		}
		fullNames = append(fullNames, job.FullNameFrom(projectName, jobName))
	}
	return fullNames, nil
}

func (j JobRepository) Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error {
	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		jobFullName := projectName.String() + "/" + jobName.String()
		if err := j.deleteUpstreams(tx, []string{jobFullName}); err != nil {
			return err
		}

		if cleanHistory {
			return j.hardDelete(tx, projectName, jobName)
		}
		return j.softDelete(tx, projectName, jobName)
	})
}

func (JobRepository) hardDelete(tx *gorm.DB, projectName tenant.ProjectName, jobName job.Name) error {
	query := `
DELETE 
FROM job
WHERE project_name = ? AND name = ?
`
	result := tx.Exec(query, projectName.String(), jobName.String())
	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during job deletion", result.Error)
	}
	return nil
}

func (JobRepository) softDelete(tx *gorm.DB, projectName tenant.ProjectName, jobName job.Name) error {
	query := `
UPDATE job
SET deleted_at = current_timestamp
WHERE project_name = ? AND name = ?
`
	result := tx.Exec(query, projectName.String(), jobName.String())
	if result.Error != nil {
		return errors.Wrap(job.EntityJob, "error during job deletion", result.Error)
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

func (j JobRepository) GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination job.ResourceURN) ([]*dto.Downstream, error) {
	query := `
SELECT
	name as job_name, project_name, namespace_name, task_name
FROM job
WHERE project_name = ? AND ? = ANY(sources);
`

	var storeDownstreams []Downstream
	if err := j.db.WithContext(ctx).Raw(query, projectName.String(), destination.String()).
		Scan(&storeDownstreams).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting downstream by destination", err)
	}

	var downstreams []*dto.Downstream
	for _, storeDownstream := range storeDownstreams {
		downstreams = append(downstreams, &dto.Downstream{
			Name:          storeDownstream.JobName,
			ProjectName:   storeDownstream.ProjectName,
			NamespaceName: storeDownstream.NamespaceName,
			TaskName:      storeDownstream.TaskName,
		})
	}
	return downstreams, nil
}

type Downstream struct {
	JobName       string `json:"job_name"`
	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`
	TaskName      string `json:"task_name"`
}

func (j JobRepository) GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*dto.Downstream, error) {
	query := `
SELECT
	j.name as job_name, j.project_name, j.namespace_name, j.task_name
FROM job_upstream ju
JOIN job j ON (ju.job_name = j.name AND ju.project_name = j.project_name)
WHERE upstream_project_name=? AND upstream_job_name=?;
`

	var storeDownstreams []Downstream
	if err := j.db.WithContext(ctx).Raw(query, projectName.String(), jobName.String()).
		Scan(&storeDownstreams).Error; err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting downstream by job name", err)
	}

	var downstreams []*dto.Downstream
	for _, storeDownstream := range storeDownstreams {
		downstreams = append(downstreams, &dto.Downstream{
			Name:          storeDownstream.JobName,
			ProjectName:   storeDownstream.ProjectName,
			NamespaceName: storeDownstream.NamespaceName,
			TaskName:      storeDownstream.TaskName,
		})
	}
	return downstreams, nil
}
