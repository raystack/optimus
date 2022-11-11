package dag

import (
	"time"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
)

const (
	EntitySchedulerAirflow = "schedulerAirflow"
)

type TemplateContext struct {
	Job *job_run.Job

	Tenant          tenant.Tenant
	Version         string
	SlaMissDuration int64
	Hostname        string
	ExecutorTask    string
	ExecutorHook    string

	RuntimeConfig RuntimeConfig
	Task          Task
	Hooks         Hooks
	Priority      int
}

type Task struct {
	Name  string
	Image string
}

func PrepareTask(job *job_run.Job, pluginRepo PluginRepo) (Task, error) {
	plugin, err := pluginRepo.GetByName(job.Task.Name)
	if err != nil {
		return Task{}, errors.NotFound(EntitySchedulerAirflow, "plugin not found for "+job.Task.Name)
	}

	info := plugin.Info()

	return Task{
		Name:  info.Name,
		Image: info.Image,
	}, nil
}

type Hook struct {
	Name       string
	Image      string
	IsFailHook bool
}

type Hooks struct {
	Pre          []Hook
	Post         []Hook
	Fail         []Hook
	Dependencies map[string]string
}

func (h Hooks) List() []Hook {
	list := h.Pre
	list = append(list, h.Post...)
	list = append(list, h.Fail...)
	return list
}

func PrepareHooksForJob(job *job_run.Job, pluginRepo PluginRepo) (Hooks, error) {
	var hooks Hooks
	hooks.Dependencies = map[string]string{}

	for _, h := range job.Hooks {
		hook, err := pluginRepo.GetByName(h.Name)
		if err != nil {
			return Hooks{}, errors.NotFound("schedulerAirflow", "hook not found for name "+h.Name)
		}

		info := hook.Info()
		hk := Hook{
			Name:  h.Name,
			Image: info.Image,
		}
		switch info.HookType {
		case models.HookTypePre:
			hooks.Pre = append(hooks.Pre, hk)
		case models.HookTypePost:
			hooks.Post = append(hooks.Post, hk)
		case models.HookTypeFail:
			hk.IsFailHook = true
			hooks.Fail = append(hooks.Fail, hk)
		}

		for _, before := range info.DependsOn { // If we do not have a hook for before, raise error
			hooks.Dependencies[before] = h.Name
		}
	}

	return hooks, nil
}

type RuntimeConfig struct {
	Resource *Resource
	Airflow  AirflowConfig
}

func SetupRuntimeConfig(job *job_run.Job) RuntimeConfig {
	if job.Metadata == nil {
		return RuntimeConfig{}
	}

	runtimeConf := RuntimeConfig{
		Airflow: ToAirflowConfig(job.Metadata.Scheduler),
	}
	if resource := ToResource(job.Metadata.Resource); resource != nil {
		runtimeConf.Resource = resource
	}
	return runtimeConf
}

type Resource struct {
	Request *ResourceConfig
	Limit   *ResourceConfig
}

func ToResource(resource *job_run.Resource) *Resource {
	if resource == nil {
		return nil
	}
	req := ToResourceConfig(resource.Request)
	limit := ToResourceConfig(resource.Limit)
	if req == nil && limit == nil {
		return nil
	}
	res := &Resource{}
	if req != nil {
		res.Request = req
	}
	if limit != nil {
		res.Limit = limit
	}
	return res
}

type ResourceConfig struct {
	CPU    string
	Memory string
}

func ToResourceConfig(config *job_run.ResourceConfig) *ResourceConfig {
	if config == nil {
		return nil
	}
	if config.CPU == "" && config.Memory == "" {
		return nil
	}
	return &ResourceConfig{
		CPU:    config.CPU,
		Memory: config.Memory,
	}
}

type AirflowConfig struct {
	Pool  string
	Queue string
}

func ToAirflowConfig(schedulerConf map[string]string) AirflowConfig {
	conf := AirflowConfig{}
	if pool, ok := schedulerConf["pool"]; ok {
		conf.Pool = pool
	}
	if queue, ok := schedulerConf["queue"]; ok {
		conf.Queue = queue
	}
	return conf
}

func SlaMissDuration(job *job_run.Job) (int64, error) {
	var slaMissDurationInSec int64
	for _, notify := range job.Alerts { // We are ranging and picking one value
		if notify.On == job_run.SLAMissEvent {
			duration, ok := notify.Config["duration"]
			if !ok {
				continue
			}

			dur, err := time.ParseDuration(duration)
			if err != nil {
				return 0, errors.InvalidArgument(EntitySchedulerAirflow, "failed to parse sla_miss duration "+duration)
			}
			slaMissDurationInSec = int64(dur.Seconds())
		}
	}
	return slaMissDurationInSec, nil
}
