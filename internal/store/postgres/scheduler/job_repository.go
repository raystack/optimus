package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/utils"
)

type JobRepository struct {
	db *gorm.DB
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
		State:          j.UpstreamState,
	}, err
}

type Job struct {
	ID          uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Name        string    `gorm:"not null" json:"name"`
	Version     int
	Owner       string
	Description string
	Labels      datatypes.JSON

	StartDate time.Time
	EndDate   *time.Time
	Interval  string

	// Behavior
	DependsOnPast bool `json:"depends_on_past"`
	CatchUp       bool `json:"catch_up"`
	Retry         datatypes.JSON
	Alert         datatypes.JSON

	// Upstreams
	StaticUpstreams pq.StringArray `gorm:"type:varchar(220)[]" json:"static_upstreams"`

	// ExternalUpstreams
	HTTPUpstreams datatypes.JSON `json:"http_upstreams"`

	TaskName   string
	TaskConfig datatypes.JSON

	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string

	Assets   datatypes.JSON
	Hooks    datatypes.JSON
	Metadata datatypes.JSON

	Destination string
	Sources     pq.StringArray `gorm:"type:varchar(300)[]"`

	ProjectName   string `json:"project_name"`
	NamespaceName string `json:"namespace_name"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func (j *Job) toJob() (*scheduler.Job, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}

	taskConfig := map[string]string{}
	if err := json.Unmarshal(j.TaskConfig, &taskConfig); err != nil {
		return nil, err
	}

	var hookConfig []*scheduler.Hook
	if err := json.Unmarshal(j.Hooks, &hookConfig); err != nil {
		return nil, err
	}

	assets := map[string]string{}
	if err := json.Unmarshal(j.Assets, &assets); err != nil {
		return nil, err
	}

	return &scheduler.Job{
		Name:        scheduler.JobName(j.Name),
		Tenant:      t,
		Destination: j.Destination,
		Task: &scheduler.Task{
			Name:   j.TaskName,
			Config: taskConfig,
		},
		Hooks:  hookConfig,
		Assets: assets,
	}, err
}

func (j *Job) toJobWithDetails() (*scheduler.JobWithDetails, error) {
	job, err := j.toJob()
	if err != nil {
		return nil, err
	}

	var labels map[string]string
	if err := json.Unmarshal(j.Labels, &labels); err != nil {
		return nil, err
	}

	var retry scheduler.Retry
	if err := json.Unmarshal(j.Retry, &retry); err != nil {
		return nil, err
	}
	var alerts []scheduler.Alert
	if err := json.Unmarshal(j.Alert, &alerts); err != nil {
		return nil, err
	}

	return &scheduler.JobWithDetails{
		Name: job.Name,
		Job:  job,
		JobMetadata: &scheduler.JobMetadata{
			Version:     j.Version,
			Owner:       j.Owner,
			Description: j.Description,
			Labels:      labels,
		},
		Schedule: &scheduler.Schedule{
			DependsOnPast: j.DependsOnPast,
			CatchUp:       j.CatchUp,
			StartDate:     j.StartDate,
			EndDate:       j.EndDate,
		},
		Retry:  &retry,
		Alerts: alerts,
	}, err
}

func (j *JobRepository) GetJob(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.Job, error) {
	var spec Job

	getJobByNameAtProject := `SELECT * FROM job WHERE name = ? AND project_name = ?`
	err := j.db.WithContext(ctx).Raw(getJobByNameAtProject, jobName.String(), projectName.String()).
		First(&spec).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "unable to find job:"+jobName.String()+" in project:"+projectName.String())
		}
		return nil, err
	}
	return spec.toJob()
}
func (j *JobRepository) GetJobDetails(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName) (*scheduler.JobWithDetails, error) {
	var spec Job
	getJobByNameAtProject := `SELECT * FROM job WHERE name = ? AND project_name = ?`
	err := j.db.WithContext(ctx).Raw(getJobByNameAtProject, jobName.String(), projectName.String()).
		First(&spec).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "unable to find job:"+jobName.String()+" in project:"+projectName.String())
		}
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
			multiError.Append(err) // TODO: ask sandeep should we append errors or return
			continue
		}
		jobUpstreamGroup[upstream.JobName] = append(jobUpstreamGroup[upstream.JobName], schedulerUpstream)
	}
	return jobUpstreamGroup, errors.MultiToError(multiError)
}

func (j *JobRepository) getJobsUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []string) (map[string][]*scheduler.JobUpstream, error) {
	var jobsUpstreams []JobUpstreams
	jobNameListString := strings.Join(jobNames, "', '")
	getJobUpstreamsByNameAtProject := fmt.Sprintf("SELECT * FROM job_upstream WHERE project_name = '%s' and job_name in ('%s')", projectName.String(), jobNameListString)
	err := j.db.WithContext(ctx).Raw(getJobUpstreamsByNameAtProject, projectName.String()).Find(&jobsUpstreams).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "unable to find jobsUpstreams in project:"+projectName.String()+" for:"+jobNameListString)
		}
		return nil, err
	}
	return groupUpstreamsByJobName(jobsUpstreams)
}

func (j *JobRepository) GetAllWithUpstreams(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
	var specs []Job
	getJobByNameAtProject := `SELECT * FROM job WHERE project_name = ?`
	err := j.db.WithContext(ctx).Raw(getJobByNameAtProject, projectName.String()).Find(&specs).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "unable to find jobs in project:"+projectName.String())
		}
		return nil, err
	}
	jobsMap := map[string]*scheduler.JobWithDetails{}
	var jobNameList []string
	multiError := errors.NewMultiError("errorInGetAllWithUpstreams")
	for _, spec := range specs {
		job, err := spec.toJobWithDetails()
		if err != nil {
			multiError.Append(err)
			continue
		}
		jobNameList = append(jobNameList, job.GetName())
		jobsMap[job.GetName()] = job
	}

	// TODO: ask sandeep should use what is resolved and return a partial error msg, or stop the operation and return error
	jobUpstreamGroupedByName, err := j.getJobsUpstreams(ctx, projectName, jobNameList)
	if err != nil {
		return nil, err
	}
	for jobName, upstreamList := range jobUpstreamGroupedByName {
		jobsMap[jobName].Upstreams.UpstreamJobs = upstreamList
	}

	return utils.MapToList[*scheduler.JobWithDetails](jobsMap), err
}

func NewJobProviderRepository(db *gorm.DB) *JobRepository {
	return &JobRepository{
		db: db,
	}
}
