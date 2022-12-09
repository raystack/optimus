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
	"github.com/odpf/optimus/internal/models"
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
		taskConfig := map[string]map[string]string{} // todo: later make a simple map
		if err := json.Unmarshal(j.TaskConfig, &taskConfig); err != nil {
			return nil, err
		}
		schedulerJob.Task = &scheduler.Task{
			Name:   j.TaskName,
			Config: taskConfig["Configs"], // todo: discuss and get sorted
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
		assetsTemp := []map[string]string{} // todo: fix, assets temp should not be needed after JOB BC change
		if err := json.Unmarshal(j.Assets, &assetsTemp); err != nil {
			return nil, err
		}
		assets := map[string]string{}
		for _, m := range assetsTemp {
			assets[m["Name"]] = m["Value"]
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
			EndDate:       j.EndDate,
		},
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

func (j *JobRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.JobWithDetails, error) {
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

func NewJobProviderRepository(db *gorm.DB) *JobRepository {
	return &JobRepository{
		db: db,
	}
}
