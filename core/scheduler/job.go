package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/models"
)

type (
	JobName      string
	OperatorType string
)

func (o OperatorType) String() string {
	return string(o)
}

const (
	EntityJobRun = "jobRun"

	OperatorTask   OperatorType = "task"
	OperatorSensor OperatorType = "sensor"
	OperatorHook   OperatorType = "hook"
)

func JobNameFrom(name string) (JobName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityJobRun, "job name is empty")
	}

	return JobName(name), nil
}

func (n JobName) String() string {
	return string(n)
}

type Job struct {
	Name   JobName
	Tenant tenant.Tenant

	Destination string
	Task        *Task
	Hooks       []*Hook
	Window      models.Window
	Assets      map[string]string
}

func (j *Job) GetHook(hookName string) (*Hook, error) {
	for _, hook := range j.Hooks {
		if hook.Name == hookName {
			return hook, nil
		}
	}
	return nil, errors.NotFound(EntityJobRun, "hook:"+hookName)
}

type Task struct {
	Name   string
	Config map[string]string
}

type Hook struct {
	Name   string
	Config map[string]string
}

// JobWithDetails contains the details for a job
type JobWithDetails struct {
	Name JobName

	Job           *Job
	JobMetadata   *JobMetadata
	Schedule      *Schedule
	Retry         Retry
	Alerts        []Alert
	RuntimeConfig RuntimeConfig
	Priority      int
	Upstreams     Upstreams
}

func (j JobWithDetails) GetName() string {
	return j.Name.String()
}

func GroupJobsByTenant(j []*JobWithDetails) map[tenant.Tenant][]*JobWithDetails {
	jobsGroup := make(map[tenant.Tenant][]*JobWithDetails)
	for _, job := range j {
		tnnt := job.Job.Tenant
		jobsGroup[tnnt] = append(jobsGroup[tnnt], job)
	}
	return jobsGroup
}

func (j JobWithDetails) SLADuration() (int64, error) {
	for _, notify := range j.Alerts {
		if notify.On == EventCategorySLAMiss {
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

type JobMetadata struct {
	Version     int
	Owner       string
	Description string
	Labels      map[string]string
}

type Schedule struct {
	DependsOnPast bool
	CatchUp       bool
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
}

func (j *JobWithDetails) GetLabelsAsString() string {
	labels := ""
	for k, v := range j.JobMetadata.Labels {
		labels += fmt.Sprintf("%s=%s,", strings.TrimSpace(k), strings.TrimSpace(v))
	}
	return strings.TrimRight(labels, ",")
}

type Retry struct {
	ExponentialBackoff bool
	Count              int
	Delay              int32
}

type Alert struct {
	On       JobEventCategory
	Channels []string
	Config   map[string]string
}

type RuntimeConfig struct {
	Resource  *Resource
	Scheduler map[string]string
}

type Resource struct {
	Request *ResourceConfig
	Limit   *ResourceConfig
}

type ResourceConfig struct {
	CPU    string
	Memory string
}

type Upstreams struct {
	HTTP         []*HTTPUpstreams
	UpstreamJobs []*JobUpstream
}

type HTTPUpstreams struct {
	Name    string
	URL     string
	Headers map[string]string
	Params  map[string]string
}

type JobUpstream struct {
	JobName        string
	Host           string
	TaskName       string        // TODO: remove after airflow migration
	DestinationURN string        //- bigquery://pilot.playground.table
	Tenant         tenant.Tenant // Current or external tenant
	Type           string
	External       bool
	State          string
}
