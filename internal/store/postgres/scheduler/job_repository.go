package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/internal/utils"
)

const (
	jobColumns = `id, name, version, owner, description,
				labels, start_date, end_date, interval, depends_on_past,
				catch_up, retry, alert, static_upstreams, http_upstreams,
				task_name, task_config, window_size, window_offset, window_truncate_to,
				assets, hooks, metadata, destination, sources,
				project_name, namespace_name, created_at, updated_at`
	upstreamColumns = `
    job_name, project_name, upstream_job_name, upstream_project_name,
    upstream_namespace_name, upstream_resource_urn, upstream_task_name, upstream_type, upstream_external`
)

type JobRepository struct {
	db *pgxpool.Pool
}

type JobUpstreams struct {
	JobID                 uuid.UUID
	JobName               string
	ProjectName           string
	UpstreamJobID         uuid.UUID
	UpstreamJobName       string
	UpstreamResourceUrn   string
	UpstreamProjectName   string
	UpstreamNamespaceName string
	UpstreamTaskName      string
	UpstreamHost          string
	UpstreamType          string
	UpstreamState         string
	UpstreamExternal      bool

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (j *JobUpstreams) toJobUpstreams() (*scheduler.JobUpstream, error) {
	t, err := tenant.NewTenant(j.UpstreamProjectName, j.UpstreamNamespaceName)
	if err != nil {
		return nil, err
	}

	return &scheduler.JobUpstream{
		JobName:        j.UpstreamJobName,
		Host:           j.UpstreamHost,
		TaskName:       j.UpstreamTaskName,
		DestinationURN: j.UpstreamResourceUrn,
		Tenant:         t,
		Type:           j.UpstreamType,
		External:       j.UpstreamExternal,
		State:          j.UpstreamState,
	}, err
}

type Job struct {
	ID          uuid.UUID
	Name        string
	Version     int
	Owner       string
	Description string
	Labels      map[string]string

	StartDate time.Time
	EndDate   *time.Time
	Interval  string

	// Behavior
	DependsOnPast bool `json:"depends_on_past"`
	CatchUp       bool `json:"catch_up"`
	Retry         json.RawMessage
	Alert         json.RawMessage

	// Upstreams
	StaticUpstreams pq.StringArray `json:"static_upstreams"`

	// ExternalUpstreams
	HTTPUpstreams json.RawMessage `json:"http_upstreams"`

	TaskName   string
	TaskConfig map[string]string

	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string

	Assets   map[string]string
	Hooks    json.RawMessage
	Metadata json.RawMessage

	Destination string
	Sources     pq.StringArray

	ProjectName   string
	NamespaceName string

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime
}

func (j *Job) toJob() (*scheduler.Job, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	window, err := models.NewWindow(j.Version, j.WindowTruncateTo, j.WindowOffset, j.WindowSize)
	if err != nil {
		return nil, err
	}
	schedulerJob := scheduler.Job{
		Name:        scheduler.JobName(j.Name),
		Tenant:      t,
		Destination: j.Destination,
		Window:      window,
		Assets:      j.Assets,
		Task: &scheduler.Task{
			Name:   j.TaskName,
			Config: j.TaskConfig,
		},
	}

	if j.Hooks != nil {
		var hookConfig []*scheduler.Hook
		if err := json.Unmarshal(j.Hooks, &hookConfig); err != nil {
			return nil, err
		}
		schedulerJob.Hooks = hookConfig
	}

	return &schedulerJob, nil
}

func (j *Job) toJobWithDetails() (*scheduler.JobWithDetails, error) {
	job, err := j.toJob()
	if err != nil {
		return nil, err
	}

	schedulerJobWithDetails := &scheduler.JobWithDetails{
		Name: job.Name,
		Job:  job,
		JobMetadata: &scheduler.JobMetadata{
			Version:     j.Version,
			Owner:       j.Owner,
			Description: j.Description,
			Labels:      j.Labels,
		},
		Schedule: &scheduler.Schedule{
			DependsOnPast: j.DependsOnPast,
			CatchUp:       j.CatchUp,
			StartDate:     j.StartDate,
			Interval:      j.Interval,
		},
	}
	if !j.EndDate.IsZero() {
		schedulerJobWithDetails.Schedule.EndDate = j.EndDate
	}

	if j.Retry != nil {
		if err := json.Unmarshal(j.Retry, &schedulerJobWithDetails.Retry); err != nil {
			return nil, err
		}
	}

	if j.Alert != nil {
		var alerts []scheduler.Alert
		if err := json.Unmarshal(j.Alert, &alerts); err != nil {
			return nil, err
		}
		schedulerJobWithDetails.Alerts = alerts
	}

	return schedulerJobWithDetails, nil
}

func FromRow(row pgx.Row) (*Job, error) {
	var js Job

	err := row.Scan(&js.ID, &js.Name, &js.Version, &js.Owner, &js.Description,
		&js.Labels, &js.StartDate, &js.EndDate, &js.Interval, &js.DependsOnPast,
		&js.CatchUp, &js.Retry, &js.Alert, &js.StaticUpstreams, &js.HTTPUpstreams,
		&js.TaskName, &js.TaskConfig, &js.WindowSize, &js.WindowOffset, &js.WindowTruncateTo,
		&js.Assets, &js.Hooks, &js.Metadata, &js.Destination, &js.Sources,
		&js.ProjectName, &js.NamespaceName, &js.CreatedAt, &js.UpdatedAt, &js.DeletedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job not found")
		}

		return nil, errors.Wrap(scheduler.EntityJobRun, "error in reading row for resource", err)
	}

	return &js, nil
}

func (j *JobRepository) GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2`
	spec, err := FromRow(j.db.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
	if err != nil {
		return nil, err
	}
	return spec.toJob()
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2`
	spec, err := FromRow(j.db.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
	if err != nil {
		return nil, err
	}
	return spec.toJobWithDetails()
}

func groupUpstreamsByJobName(jobUpstreams []JobUpstreams) (map[string][]*scheduler.JobUpstream, error) {
	multiError := errors.NewMultiError("errorsInGroupUpstreamsByJobName")
	jobUpstreamGroup := map[string][]*scheduler.JobUpstream{}

	for _, upstream := range jobUpstreams {
		schedulerUpstream, err := upstream.toJobUpstreams()
		if err != nil {
			msg := fmt.Sprintf("unable to parse upstream:%s for job:%s", upstream.UpstreamJobName, upstream.JobName)
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, msg, err))
			continue
		}
		jobUpstreamGroup[upstream.JobName] = append(jobUpstreamGroup[upstream.JobName], schedulerUpstream)
	}
	return jobUpstreamGroup, multiError.ToErr()
}

func (j *JobRepository) getJobsUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []string) (map[string][]*scheduler.JobUpstream, error) {
	getJobUpstreamsByNameAtProject := "SELECT " + upstreamColumns + " FROM job_upstream WHERE project_name = $1 and job_name = any ($2)"
	rows, err := j.db.Query(ctx, getJobUpstreamsByNameAtProject, projectName, jobNames)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting job with upstreams", err)
	}
	defer rows.Close()

	var upstreams []JobUpstreams
	for rows.Next() {
		var jwu JobUpstreams
		err := rows.Scan(&jwu.JobName, &jwu.ProjectName, &jwu.UpstreamJobName, &jwu.UpstreamProjectName,
			&jwu.UpstreamNamespaceName, &jwu.UpstreamResourceUrn, &jwu.UpstreamTaskName, &jwu.UpstreamType, &jwu.UpstreamExternal)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(scheduler.EntityJobRun, "job upstream not found")
			}

			return nil, errors.Wrap(scheduler.EntityJobRun, "error in reading row for resource", err)
		}
		upstreams = append(upstreams, jwu)
	}

	return groupUpstreamsByJobName(upstreams)
}

func (j *JobRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE project_name = $1`
	rows, err := j.db.Query(ctx, getJobByNameAtProject, projectName)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "error while getting all jobs", err)
	}
	defer rows.Close()

	jobsMap := map[string]*scheduler.JobWithDetails{}
	var jobNameList []string
	multiError := errors.NewMultiError("errorInGetAll")
	for rows.Next() {
		spec, err := FromRow(rows)
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}

		job, err := spec.toJobWithDetails()
		if err != nil {
			multiError.Append(errors.Wrap(scheduler.EntityJobRun, "error parsing job:"+spec.Name, err))
			continue
		}
		jobNameList = append(jobNameList, job.GetName())
		jobsMap[job.GetName()] = job
	}

	jobUpstreamGroupedByName, err := j.getJobsUpstreams(ctx, projectName, jobNameList)
	multiError.Append(err)

	for jobName, upstreamList := range jobUpstreamGroupedByName {
		jobsMap[jobName].Upstreams.UpstreamJobs = upstreamList
	}

	return utils.MapToList[*scheduler.JobWithDetails](jobsMap), errors.MultiToError(multiError)
}

func NewJobProviderRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{
		db: pool,
	}
}
