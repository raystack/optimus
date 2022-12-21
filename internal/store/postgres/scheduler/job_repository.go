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
	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/internal/utils"
)

type JobRepository struct {
	pool *pgxpool.Pool
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

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func UpstreamFromRow(row pgx.Row) (*JobUpstreams, error) {
	var js JobUpstreams

	err := row.Scan(&js.JobName, &js.ProjectName, &js.UpstreamJobName, &js.UpstreamResourceUrn,
		&js.UpstreamProjectName, &js.UpstreamNamespaceName, &js.UpstreamTaskName, &js.UpstreamHost,
		&js.UpstreamType, &js.UpstreamState, &js.UpstreamExternal)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(job.EntityJob, "job upstream not found")
		}

		return nil, errors.Wrap(resource.EntityResource, "error in reading row for resource", err)
	}

	return &js, nil
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
	ID          uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name        string    `gorm:"not null" json:"name"`
	Version     int
	Owner       string
	Description string
	Labels      json.RawMessage

	StartDate time.Time
	EndDate   *time.Time
	Interval  string

	// Behavior
	DependsOnPast bool `json:"depends_on_past"`
	CatchUp       bool `json:"catch_up"`
	Retry         json.RawMessage
	Alert         json.RawMessage

	// Upstreams
	StaticUpstreams pq.StringArray `gorm:"type:varchar(220)[]" json:"static_upstreams"`

	// ExternalUpstreams
	HTTPUpstreams json.RawMessage `json:"http_upstreams"`

	TaskName   string
	TaskConfig json.RawMessage

	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string

	Assets   json.RawMessage
	Hooks    json.RawMessage
	Metadata json.RawMessage

	Destination string
	Sources     pq.StringArray `gorm:"type:varchar(300)[]"`

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
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
	}

	if j.TaskConfig != nil {
		taskConfig := map[string]string{}
		if err := json.Unmarshal(j.TaskConfig, &taskConfig); err != nil {
			return nil, err
		}
		schedulerJob.Task = &scheduler.Task{
			Name:   j.TaskName,
			Config: taskConfig,
		}
	}

	if j.Hooks != nil {
		var hookConfig []*scheduler.Hook
		if err := json.Unmarshal(j.Hooks, &hookConfig); err != nil {
			return nil, err
		}
		schedulerJob.Hooks = hookConfig
	}

	if j.Assets != nil {
		assets := map[string]string{}
		if err := json.Unmarshal(j.Assets, &assets); err != nil {
			return nil, err
		}
		schedulerJob.Assets = assets
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
	if j.Labels != nil {
		var labels map[string]string
		if err := json.Unmarshal(j.Labels, &labels); err != nil {
			return nil, err
		}
		schedulerJobWithDetails.JobMetadata.Labels = labels
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
	getJobByNameAtProject := `SELECT * FROM job WHERE name = $1 AND project_name = $2`
	spec, err := FromRow(j.pool.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
	if err != nil {
		return nil, err
	}
	return spec.toJob()
}

func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	getJobByNameAtProject := `SELECT * FROM job WHERE name = $1 AND project_name = $2`
	spec, err := FromRow(j.pool.QueryRow(ctx, getJobByNameAtProject, jobName, projectName))
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
			multiError.Append(
				errors.Wrap(
					scheduler.EntityJobRun,
					fmt.Sprintf("unable to parse upstream:%s for job:%s", upstream.UpstreamJobName, upstream.JobName),
					err))
			continue
		}
		jobUpstreamGroup[upstream.JobName] = append(jobUpstreamGroup[upstream.JobName], schedulerUpstream)
	}
	return jobUpstreamGroup, errors.MultiToError(multiError)
}

func (j *JobRepository) getJobsUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []string) (map[string][]*scheduler.JobUpstream, error) {
	getJobUpstreamsByNameAtProject := "SELECT * FROM job_upstream WHERE project_name = $1 and job_name = any ($2)"
	rows, err := j.pool.Query(ctx, getJobUpstreamsByNameAtProject, projectName, jobNames)
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
	var specs []Job
	// getJobByNameAtProject := `SELECT * FROM job WHERE project_name = $1`
	//rows, err := j.pool.Query(ctx, getJobByNameAtProject, projectName)
	//if err != nil {
	//	return nil, errors.Wrap(job.EntityJob, "error while getting all jobs", err)
	//}

	jobsMap := map[string]*scheduler.JobWithDetails{}
	var jobNameList []string
	multiError := errors.NewMultiError("errorInGetAll")
	for _, spec := range specs {
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
		pool: pool,
	}
}
