package job_run

import (
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

type JobNotifierConfig struct {
	On       JobEventCategory
	Config   map[string]string
	Channels []string
}

type Job struct {
	JobName JobName
	tenant  tenant.Tenant

	destination  string
	task         *Task
	hooks        []*Hook
	window       models.Window
	assets       map[string]string
	notifyConfig JobNotifierConfig
}

func (j *Job) Tenant() tenant.Tenant {
	return j.tenant
}

func (j *Job) SLADuration() (int64, error) {
	return 0, nil
}

func (j *Job) Destination() string {
	return j.destination
}

func (j *Job) Task() *Task {
	return j.task
}

func (j *Job) GetHook(hookName string) (*Hook, error) {
	for _, hook := range j.hooks {
		if hook.name == hookName {
			return hook, nil
		}
	}
	return nil, errors.NotFound(EntityJobRun, "hook not found in job "+hookName)
}

func (j *Job) Window() models.Window {
	return j.window
}

func (j *Job) Assets() map[string]string {
	return j.assets
}

type JobWithDetails struct {
}

type Task struct {
	name   string
	config map[string]string
}

func (t *Task) Name() string {
	return t.name
}
func (t *Task) Config() map[string]string {
	return t.config
}

type Hook struct {
	name   string
	config map[string]string
}

func (h *Hook) Name() string {
	return h.name
}

func (h *Hook) Config() map[string]string {
	return h.config
}
