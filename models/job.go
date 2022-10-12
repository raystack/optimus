package models

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/odpf/optimus/api/writer"
)

var (
	ErrNoSuchSpec  = errors.New("spec not found")
	ErrNoSuchJob   = errors.New("job not found")
	ErrNoJobs      = errors.New("no job found")
	ErrNoResources = errors.New("no resources found")
	ErrNoSuchAsset = errors.New("asset not found")
	ErrNoSuchHook  = errors.New("hook not found")
)

const (
	JobDatetimeLayout = "2006-01-02"

	// JobSpecDependencyTypeIntra represents dependency within a project
	JobSpecDependencyTypeIntra JobSpecDependencyType = "intra"
	// JobSpecDependencyTypeInter represents dependency within optimus but cross project
	JobSpecDependencyTypeInter JobSpecDependencyType = "inter"
	// JobSpecDependencyTypeExtra represents dependency outside optimus
	JobSpecDependencyTypeExtra JobSpecDependencyType = "extra"

	SLAMissEvent    JobEventType = "sla_miss"
	JobFailureEvent JobEventType = "failure"

	JobStartEvent   JobEventType = "job_start"
	JobFailEvent    JobEventType = "job_fail"
	JobSuccessEvent JobEventType = "job_success"

	TaskStartEvent   JobEventType = "task_start"
	TaskRetryEvent   JobEventType = "task_retry"
	TaskFailEvent    JobEventType = "task_fail"
	TaskSuccessEvent JobEventType = "task_success"

	HookStartEvent   JobEventType = "hook_start"
	HookRetryEvent   JobEventType = "hook_retry"
	HookFailEvent    JobEventType = "hook_fail"
	HookSuccessEvent JobEventType = "hook_success"

	SensorStartEvent   JobEventType = "sensor_start"
	SensorRetryEvent   JobEventType = "sensor_retry"
	SensorFailEvent    JobEventType = "sensor_fail"
	SensorSuccessEvent JobEventType = "sensor_success"

	JobRetryEvent JobEventType = "retry"

	JobSpecDefaultVersion = 1
)

// JobSpec represents a job
// internal representation of the job
type JobSpec struct {
	ID                   uuid.UUID
	Version              int
	Name                 string
	Description          string
	Labels               map[string]string
	Owner                string
	ResourceDestination  string
	Schedule             JobSpecSchedule
	Behavior             JobSpecBehavior
	Task                 JobSpecTask
	Dependencies         map[string]JobSpecDependency // job name to dependency
	Assets               JobAssets
	Hooks                []JobSpecHook
	Metadata             JobSpecMetadata
	ExternalDependencies ExternalDependency // external dependencies for http
	NamespaceSpec        NamespaceSpec
}

func (js JobSpec) GetName() string {
	return js.Name
}

func (js JobSpec) GetFullName() string {
	return js.GetProjectSpec().Name + "/" + js.Name
}

func (js JobSpec) SLADuration() (int64, error) {
	for _, notify := range js.Behavior.Notify {
		if notify.On == SLAMissEvent {
			if _, ok := notify.Config["duration"]; !ok {
				continue
			}

			dur, err := time.ParseDuration(notify.Config["duration"])
			if err != nil {
				return 0, fmt.Errorf("failed to parse sla_miss duration %s: %w", notify.Config["duration"], err)
			}
			return int64(dur.Seconds()), nil
		}
	}
	return 0, nil
}

func (js JobSpec) GetHookByName(name string) (JobSpecHook, error) {
	for _, hook := range js.Hooks {
		if hook.Unit.Info().Name == name {
			return hook, nil
		}
	}
	return JobSpecHook{}, ErrNoSuchHook
}

func (js JobSpec) GetLabelsAsString() string {
	labels := ""
	for k, v := range js.Labels {
		labels += fmt.Sprintf("%s=%s,", strings.TrimSpace(k), strings.TrimSpace(v))
	}
	return strings.TrimRight(labels, ",")
}

func (js JobSpec) GetProjectSpec() ProjectSpec {
	return js.NamespaceSpec.ProjectSpec
}

type JobSpecs []JobSpec

func (js JobSpecs) GroupJobsPerNamespace() map[string][]JobSpec {
	jobsGroup := make(map[string][]JobSpec)
	for _, jobSpec := range js {
		jobsGroup[jobSpec.NamespaceSpec.Name] = append(jobsGroup[jobSpec.NamespaceSpec.Name], jobSpec)
	}
	return jobsGroup
}

func (js JobSpecs) GroupJobsByDestination() map[string]*JobSpec {
	output := make(map[string]*JobSpec)
	for _, jobSpec := range js {
		spec := jobSpec
		output[spec.ResourceDestination] = &spec
	}
	return output
}

type JobSpecSchedule struct {
	StartDate time.Time
	EndDate   *time.Time
	Interval  string // could be empty string for no schedule
}

type JobSpecBehavior struct {
	DependsOnPast bool
	CatchUp       bool
	Retry         JobSpecBehaviorRetry
	Notify        []JobSpecNotifier
}

type JobSpecBehaviorRetry struct {
	Count              int
	Delay              time.Duration
	ExponentialBackoff bool
}

type JobSpecNotifier struct {
	On       JobEventType
	Config   map[string]string
	Channels []string
}

func (incomingEvent JobEventType) IsOfType(targetEvent JobEventType) bool {
	var failureEvents = []JobEventType{JobFailureEvent, JobFailEvent, TaskFailEvent, HookFailEvent, SensorFailEvent}

	switch targetEvent {
	case JobFailureEvent:
		for _, event := range failureEvents {
			if incomingEvent == event {
				return true
			}
		}
	case SLAMissEvent:
		if incomingEvent == SLAMissEvent {
			return true
		}
	}
	return false
}

type JobSpecTask struct {
	Unit     *Plugin `json:"-" yaml:"-"`
	Config   JobSpecConfigs
	Window   Window
	Priority int
}

type JobSpecTaskDestination struct {
	Destination string
	Type        DestinationType
}

type JobBasicInfo struct {
	Spec        JobSpec
	JobSource   []string
	Destination string
	Log         writer.BufferedLogger
}

func (jtd JobSpecTaskDestination) URN() string {
	return fmt.Sprintf(DestinationURNFormat, jtd.Type, jtd.Destination)
}

type JobSpecTaskDependencies []string

// using array to keep order, map would be more performant
type JobSpecConfigs []JobSpecConfigItem

func (j JobSpecConfigs) Get(name string) (string, bool) {
	for _, conf := range j {
		if conf.Name == name {
			return conf.Value, true
		}
	}
	return "", false
}

type JobSpecConfigItem struct {
	Name  string
	Value string
}

type JobSpecHook struct {
	Config    JobSpecConfigs
	Unit      *Plugin
	DependsOn []*JobSpecHook
}

type JobSpecAsset struct {
	Name  string
	Value string
}

type JobAssets struct {
	data []JobSpecAsset
}

func (JobAssets) FromMap(mp map[string]string) JobAssets {
	if len(mp) == 0 {
		return JobAssets{}
	}
	assets := JobAssets{
		data: make([]JobSpecAsset, 0),
	}
	for name, val := range mp {
		assets.data = append(assets.data, JobSpecAsset{
			Name:  name,
			Value: val,
		})
	}
	return assets
}

func (a *JobAssets) ToMap() map[string]string {
	if len(a.data) == 0 {
		return nil
	}
	mp := map[string]string{}
	for _, asset := range a.data {
		mp[asset.Name] = asset.Value
	}
	return mp
}

func (a *JobAssets) GetAll() []JobSpecAsset {
	return a.data
}

func (JobAssets) New(data []JobSpecAsset) *JobAssets {
	return &JobAssets{
		data: data,
	}
}

func (a *JobAssets) GetByName(name string) (JobSpecAsset, error) {
	for _, asset := range a.data {
		if name == asset.Name {
			return asset, nil
		}
	}
	return JobSpecAsset{}, ErrNoSuchAsset
}

type JobSpecDependencyType string

func (j JobSpecDependencyType) String() string {
	return string(j)
}

type JobSpecDependency struct {
	Project *ProjectSpec
	Job     *JobSpec
	Type    JobSpecDependencyType
}

type UnresolvedJobDependency struct {
	ProjectName         string
	JobName             string
	ResourceDestination string
}

type ExternalDependency struct {
	HTTPDependencies    []HTTPDependency
	OptimusDependencies []OptimusDependency
}

type OptimusDependency struct {
	Name    string
	Host    string
	Headers map[string]string

	ProjectName   string
	NamespaceName string
	JobName       string
	TaskName      string
}

type HTTPDependency struct {
	Name          string
	RequestParams map[string]string
	URL           string
	Headers       map[string]string
}

// JobService provides a high-level operations on DAGs
type JobService interface {
	// Create constructs a Job and commits it to a storage
	Create(context.Context, NamespaceSpec, JobSpec) (JobSpec, error)

	CreateAndDeploy(context.Context, NamespaceSpec, []JobSpec, writer.LogWriter) (DeploymentID, error)

	// GetByName fetches a Job by name for a specific namespace
	GetByName(context.Context, string, NamespaceSpec) (JobSpec, error)
	// GetAll reads all job specifications of the given namespace
	GetAll(context.Context, NamespaceSpec) ([]JobSpec, error)
	// Delete deletes a job spec from all repos
	Delete(context.Context, NamespaceSpec, JobSpec) error
	// GetTaskDependencies returns job task dependency mod details
	GetTaskDependencies(context.Context, NamespaceSpec, JobSpec) (JobSpecTaskDestination,
		JobSpecTaskDependencies, error)

	// GetJobBasicInfo returns basic job info
	GetJobBasicInfo(context.Context, JobSpec) JobBasicInfo

	// Run creates a new job run for provided job spec and schedules it to execute
	// immediately
	Run(context.Context, NamespaceSpec, []JobSpec) (JobDeploymentDetail, error)

	// GetByNameForProject fetches a Job by name for a specific project
	GetByNameForProject(context.Context, string, ProjectSpec) (JobSpec, NamespaceSpec, error)
	Check(context.Context, NamespaceSpec, []JobSpec, writer.LogWriter) error
	// GetByDestination fetches a Job by destination for a specific project
	GetByDestination(ctx context.Context, projectSpec ProjectSpec, destination string) (JobSpec, error)
	// GetDownstream fetches downstream jobspecs
	GetDownstream(ctx context.Context, projectSpec ProjectSpec, jobName string) ([]JobSpec, error)
	// Refresh Redeploy current persisted state of jobs
	Refresh(ctx context.Context, projectName string, namespaceNames []string, jobNames []string, logWriter writer.LogWriter) (DeploymentID, error)
	// Deploy the requested jobs per namespace
	Deploy(context.Context, string, string, []JobSpec, writer.LogWriter) (DeploymentID, error)
	// GetDeployment getting status and result of job deployment
	GetDeployment(ctx context.Context, deployID DeploymentID) (JobDeployment, error)
	// GetByFilter gets the jobspec based on projectName, jobName, resourceDestination filters.
	GetByFilter(ctx context.Context, filter JobSpecFilter) ([]JobSpec, error)
	// GetEnrichedUpstreamJobSpec adds upstream job information to a jobSpec without persisting it in database
	GetEnrichedUpstreamJobSpec(ctx context.Context, currentSpec JobSpec, jobSources []string, logWriter writer.LogWriter) (JobSpec, []UnknownDependency, error)
	// GetDownstreamJobs reads static as well as inferred down stream dependencies
	GetDownstreamJobs(ctx context.Context, jobName, resourceDestinationURN, projectName string) ([]JobSpec, error)
}

// JobCompiler takes template file of a scheduler and after applying
// variables generates a executable input for scheduler.
type JobCompiler interface {
	Compile([]byte, NamespaceSpec, JobSpec) (Job, error)
}

// Job represents a compiled consumable item for scheduler
// this is generated from JobSpec
type Job struct {
	Name     string
	Contents []byte
}

type JobSpecFilter struct {
	ProjectName         string
	JobName             string
	ResourceDestination string
}

type JobEventType string

// JobEvent refers to status updates related to job
// posted by scheduler
type JobEvent struct {
	Type  JobEventType
	Value map[string]*structpb.Value
}

type NotifyAttrs struct {
	Namespace NamespaceSpec

	JobSpec  JobSpec
	JobEvent JobEvent

	Route string
}

type Notifier interface {
	io.Closer
	Notify(ctx context.Context, attr NotifyAttrs) error
}

// JobSpecMetadata contains metadata for a job spec
type JobSpecMetadata struct {
	Resource JobSpecResource
	Airflow  JobSpecAirflow
}

// JobSpecResource represents resource management configuration
type JobSpecResource struct {
	Request JobSpecResourceConfig
	Limit   JobSpecResourceConfig
}

// JobSpecResourceConfig represents the resource configuration
type JobSpecResourceConfig struct {
	Memory string
	CPU    string
}

// JobSpecAirflow represents additional configuration for airflow specific
type JobSpecAirflow struct {
	Pool  string
	Queue string
}

// JobQuery  represents the query to get run status from scheduler
type JobQuery struct {
	Name        string
	StartDate   time.Time
	EndDate     time.Time
	Filter      []string
	OnlyLastRun bool
}

type JobIDDependenciesPair struct {
	JobID            uuid.UUID
	DependentProject ProjectSpec
	DependentJobID   uuid.UUID
	Type             JobSpecDependencyType
}

type JobIDDependenciesPairs []JobIDDependenciesPair

func (j JobIDDependenciesPairs) GetJobDependencyMap() map[uuid.UUID][]JobIDDependenciesPair {
	jobDependencyMap := make(map[uuid.UUID][]JobIDDependenciesPair)
	for _, pair := range j {
		jobDependencyMap[pair.JobID] = append(jobDependencyMap[pair.JobID], pair)
	}
	return jobDependencyMap
}

func (j JobIDDependenciesPairs) GetExternalProjectAndDependenciesMap() map[ProjectID][]JobIDDependenciesPair {
	interDependenciesMap := make(map[ProjectID][]JobIDDependenciesPair)
	for _, dep := range j {
		if dep.Type == JobSpecDependencyTypeInter {
			interDependenciesMap[dep.DependentProject.ID] = append(interDependenciesMap[dep.DependentProject.ID], dep)
		}
	}
	return interDependenciesMap
}

type JobDeploymentStatus string

func (j JobDeploymentStatus) String() string {
	return string(j)
}

const (
	JobDeploymentStatusCancelled  JobDeploymentStatus = "Cancelled"
	JobDeploymentStatusInQueue    JobDeploymentStatus = "In Queue"
	JobDeploymentStatusInProgress JobDeploymentStatus = "In Progress"
	JobDeploymentStatusSucceed    JobDeploymentStatus = "Succeed"
	JobDeploymentStatusFailed     JobDeploymentStatus = "Failed"
)

type DeploymentID uuid.UUID

func (d DeploymentID) UUID() uuid.UUID {
	return uuid.UUID(d)
}

type JobDeployment struct {
	ID        DeploymentID
	Project   ProjectSpec
	Status    JobDeploymentStatus
	Details   JobDeploymentDetail
	CreatedAt time.Time
	UpdatedAt time.Time
}

type JobDeploymentDetail struct {
	SuccessCount                  int
	Failures                      []JobDeploymentFailure
	UnknownDependenciesPerJobName map[string][]string
}

type JobDeploymentFailure struct {
	JobName string
	Message string
}

type JobSource struct {
	JobID       uuid.UUID
	ProjectID   ProjectID
	ResourceURN string
}

type JobRunSpec struct {
	JobRunID      uuid.UUID
	JobID         uuid.UUID
	NamespaceID   uuid.UUID
	ProjectID     uuid.UUID
	ScheduledAt   time.Time
	StartTime     time.Time
	EndTime       time.Time
	Status        string
	Attempt       int
	SLAMissDelay  int
	Duration      int64
	SLADefinition int64

	Data []JobRunSpecData
}

type TaskRunSpec struct {
	TaskRunID     uuid.UUID
	JobRunID      uuid.UUID
	StartTime     time.Time
	EndTime       time.Time
	Status        string
	Attempt       int
	JobRunAttempt int
	Duration      int64
}

type SensorRunSpec struct {
	SensorRunID   uuid.UUID
	JobRunID      uuid.UUID
	StartTime     time.Time
	EndTime       time.Time
	Status        string
	Attempt       int
	JobRunAttempt int
	Duration      int64
}

type HookRunSpec struct {
	HookRunID     uuid.UUID
	JobRunID      uuid.UUID
	StartTime     time.Time
	EndTime       time.Time
	Status        string
	Attempt       int
	JobRunAttempt int
	Duration      int64
}

type UnknownDependency struct {
	JobName               string
	DependencyProjectName string
	DependencyJobName     string
}
