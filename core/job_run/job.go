package job_run

import (
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
)

type JobName string
type OperatorType string

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

	Version     int
	Owner       string
	Description string
	Labels      map[string]string
	Schedule    *Schedule
	Alerts      []*Alert
	Upstream    *Upstream
	metadata    *Metadata
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
	return nil, errors.NotFound(EntityJobRun, "hook not found in job "+hookName)
}

type Task struct {
	Name   string
	Config map[string]string
}

type Hook struct {
	Name   string
	Config map[string]string
}

type Upstream struct {
	UpstreamNames []string
	HttpUpstreams []*HTTPUpstreams
}

type HTTPUpstreams struct {
	Name    string
	Url     string
	Headers map[string]string
	Params  map[string]string
}

type Schedule struct {
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
	DependsOnPast bool
	CatchUp       bool
	Retry         *Retry
}

type Retry struct {
	Count              int
	Delay              int32
	ExponentialBackoff bool
}

type Alert struct {
	On       JobEventType
	Channels []string
	Config   map[string]string
}

type Metadata struct {
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
