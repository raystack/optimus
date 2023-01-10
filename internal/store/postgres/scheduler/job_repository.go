package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
	jobColumns = `id, name, version, owner, description, labels, schedule, alert, static_upstreams, http_upstreams,
				  task_name, task_config, window_spec, assets, hooks, metadata, destination, sources, project_name, namespace_name, created_at, updated_at`
	upstreamColumns = `
    job_name, project_name, upstream_job_name, upstream_project_name, upstream_host,
    upstream_namespace_name, upstream_resource_urn, upstream_task_name, upstream_type, upstream_external, upstream_state`
)

type JobRepository struct {
	db *pgxpool.Pool
}

type Schedule struct {
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
	DependsOnPast bool `json:"depends_on_past"`
	CatchUp       bool `json:"catch_up"`
	Retry         *Retry
}
type Retry struct {
	Count              int   `json:"count"`
	Delay              int32 `json:"delay"`
	ExponentialBackoff bool  `json:"exponential_backoff"`
}

type JobUpstreams struct {
	JobID                 uuid.UUID
	JobName               string
	ProjectName           string
	UpstreamJobID         uuid.UUID
	UpstreamJobName       sql.NullString
	UpstreamResourceUrn   sql.NullString
	UpstreamProjectName   sql.NullString
	UpstreamNamespaceName sql.NullString
	UpstreamTaskName      sql.NullString
	UpstreamHost          sql.NullString
	UpstreamType          string
	UpstreamState         string
	UpstreamExternal      sql.NullBool

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (j *JobUpstreams) toJobUpstreams() (*scheduler.JobUpstream, error) {
	t, err := tenant.NewTenant(j.UpstreamProjectName.String, j.UpstreamNamespaceName.String)
	if err != nil {
		return nil, err
	}

	return &scheduler.JobUpstream{
		JobName:        j.UpstreamJobName.String,
		Host:           j.UpstreamHost.String,
		TaskName:       j.UpstreamTaskName.String,
		DestinationURN: j.UpstreamResourceUrn.String,
		Tenant:         t,
		Type:           j.UpstreamType,
		External:       j.UpstreamExternal.Bool,
		State:          j.UpstreamState,
	}, nil
}

type Job struct {
	ID          uuid.UUID
	Name        string
	Version     int
	Owner       string
	Description string
	Labels      map[string]string

	Schedule   json.RawMessage
	WindowSpec json.RawMessage

	Alert json.RawMessage

	StaticUpstreams pq.StringArray
	HTTPUpstreams   json.RawMessage

	TaskName   string
	TaskConfig map[string]string

	Hooks json.RawMessage

	Assets map[string]string

	Metadata json.RawMessage

	Destination string
	Sources     pq.StringArray

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime
}
type Window struct {
	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string
}

func fromStorageWindow(raw []byte, jobVersion int) (models.Window, error) {
	var storageWindow Window
	if err := json.Unmarshal(raw, &storageWindow); err != nil {
		return nil, err
	}

	return models.NewWindow(
		jobVersion,
		storageWindow.WindowTruncateTo,
		storageWindow.WindowOffset,
		storageWindow.WindowSize,
	)
}
func (j *Job) toJob() (*scheduler.Job, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	var window models.Window
	if j.WindowSpec != nil {
		window, err = fromStorageWindow(j.WindowSpec, j.Version)
		if err != nil {
			return nil, err
		}
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
	var storageSchedule Schedule
	if err := json.Unmarshal(j.Schedule, &storageSchedule); err != nil {
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
			DependsOnPast: storageSchedule.DependsOnPast,
			CatchUp:       storageSchedule.CatchUp,
			StartDate:     storageSchedule.StartDate,
			Interval:      storageSchedule.Interval,
		},
	}
	if !(storageSchedule.EndDate == nil || storageSchedule.EndDate.IsZero()) {
		schedulerJobWithDetails.Schedule.EndDate = storageSchedule.EndDate
	}

	if storageSchedule.Retry != nil {
		schedulerJobWithDetails.Retry = scheduler.Retry{
			ExponentialBackoff: storageSchedule.Retry.ExponentialBackoff,
			Count:              storageSchedule.Retry.Count,
			Delay:              storageSchedule.Retry.Delay,
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
		&js.Labels, &js.Schedule, &js.Alert, &js.StaticUpstreams, &js.HTTPUpstreams,
		&js.TaskName, &js.TaskConfig, &js.WindowSpec, &js.Assets, &js.Hooks, &js.Metadata, &js.Destination, &js.Sources,
		&js.ProjectName, &js.NamespaceName, &js.CreatedAt, &js.UpdatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job not found")
		}

		return nil, errors.Wrap(scheduler.EntityJobRun, "error in reading row for job", err)
	}

	return &js, nil
}

func (j *JobRepository) GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2 AND deleted_at IS NULL`
	spec, err := FromRow(j.db.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
	if err != nil {
		return nil, err
	}
	return spec.toJob()
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE name = $1 AND project_name = $2 AND deleted_at IS NULL`
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
		if upstream.UpstreamState != "resolved" {
			if strings.EqualFold(upstream.UpstreamType, "static") {
				multiError.Append(errors.NewError(errors.ErrInvalidState, scheduler.EntityJobRun, "unresolved upstream "+upstream.UpstreamJobName.String+" for "+upstream.JobName))
			}
			continue
		}
		schedulerUpstream, err := upstream.toJobUpstreams()
		if err != nil {
			msg := fmt.Sprintf("unable to parse upstream:%s for job:%s", upstream.UpstreamJobName.String, upstream.JobName)
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
		err := rows.Scan(&jwu.JobName, &jwu.ProjectName, &jwu.UpstreamJobName, &jwu.UpstreamProjectName, &jwu.UpstreamHost,
			&jwu.UpstreamNamespaceName, &jwu.UpstreamResourceUrn, &jwu.UpstreamTaskName, &jwu.UpstreamType, &jwu.UpstreamExternal, &jwu.UpstreamState)
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
	getJobByNameAtProject := `SELECT ` + jobColumns + ` FROM job WHERE project_name = $1 AND deleted_at IS NULL`
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
	if len(jobNameList) == 0 {
		return nil, errors.NotFound(scheduler.EntityJobRun, "unable to find jobs in project:"+projectName.String())
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
