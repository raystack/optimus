package local

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gopkg.in/validator.v2"

	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
)

const (
	HoursInDay   = time.Hour * 24
	HoursInMonth = 30 * 24 * time.Hour
)

var ErrNotAMonthDuration = errors.New("invalid month string")

func init() { //nolint:gochecknoinits
	_ = validator.SetValidationFunc("isCron", utils.CronIntervalValidator)
}

// ToSpec converts the local's JobHook representation to the optimus' models.JobSpecHook
func (a JobSpecHook) ToSpec(pluginsRepo models.PluginRepository) (models.JobSpecHook, error) {
	hookUnit, err := pluginsRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, fmt.Errorf("spec reading error: %w", err)
	}
	return models.JobSpecHook{
		Config: JobSpecConfigFromMap(a.Config),
		Unit:   hookUnit,
	}, nil
}

// FromSpec converts the optimus' models.JobSpecHook representation to the local's JobHook
func (JobSpecHook) FromSpec(spec models.JobSpecHook) JobSpecHook {
	return JobSpecHook{
		Name:   spec.Unit.Info().Name,
		Config: JobSpecConfigToMap(spec.Config),
	}
}

type JobSpecAdapter struct {
	pluginRepo models.PluginRepository
}

func (adapt JobSpecAdapter) ToSpec(conf JobSpec) (models.JobSpec, error) {
	var err error

	// parse dates
	startDate, err := time.Parse(models.JobDatetimeLayout, conf.Schedule.StartDate)
	if err != nil {
		return models.JobSpec{}, err
	}
	var endDate *time.Time
	if conf.Schedule.EndDate != "" {
		end, err := time.Parse(models.JobDatetimeLayout, conf.Schedule.EndDate)
		if err != nil {
			return models.JobSpec{}, err
		}
		endDate = &end
	}

	// prep dirty dependencies and external http dependencies
	var externalDependency models.ExternalDependency
	var httpDependencies []models.HTTPDependency
	dependencies := map[string]models.JobSpecDependency{}
	for index, dep := range conf.Dependencies {
		if dep.JobName != "" {
			depType := models.JobSpecDependencyTypeIntra
			switch dep.Type {
			case string(models.JobSpecDependencyTypeIntra):
				depType = models.JobSpecDependencyTypeIntra
			case string(models.JobSpecDependencyTypeInter):
				depType = models.JobSpecDependencyTypeInter
			case string(models.JobSpecDependencyTypeExtra):
				depType = models.JobSpecDependencyTypeExtra
			}
			dependencies[dep.JobName] = models.JobSpecDependency{
				Type: depType,
			}
		}
		if dep.HTTP != nil {
			httpDep, err := prepHTTPDependency(*dep.HTTP, index)
			if err != nil {
				return models.JobSpec{}, err
			}
			httpDependencies = append(httpDependencies, httpDep)
		}
	}
	externalDependency.HTTPDependencies = httpDependencies

	// prep hooks
	var hooks []models.JobSpecHook
	for _, hook := range conf.Hooks {
		adaptHook, err := hook.ToSpec(adapt.pluginRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
		hooks = append(hooks, adaptHook)
	}

	window, err := models.NewWindow(conf.Version, conf.Task.Window.TruncateTo, conf.Task.Window.Offset, conf.Task.Window.Size)
	if err != nil {
		return models.JobSpec{}, err
	}
	if err := window.Validate(); err != nil {
		return models.JobSpec{}, err
	}

	execUnit, err := adapt.pluginRepo.GetByName(conf.Task.Name)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("spec reading error, failed to find exec unit %s: %w", conf.Task.Name, err)
	}

	labels := map[string]string{}
	for k, v := range conf.Labels {
		labels[k] = v
	}

	taskConf := models.JobSpecConfigs{}
	for name, value := range conf.Task.Config {
		taskConf = append(taskConf, models.JobSpecConfigItem{
			Name:  name,
			Value: value,
		})
	}

	retryDelayDuration := time.Duration(0)
	if conf.Behavior.Retry.Delay != 0 {
		retryDelayDuration = conf.Behavior.Retry.Delay
	}

	var jobNotifiers []models.JobSpecNotifier
	for _, notify := range conf.Behavior.Notify {
		jobNotifiers = append(jobNotifiers, models.JobSpecNotifier{
			On:       models.JobEventType(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	version := conf.Version
	if version == 0 {
		version = models.JobSpecDefaultVersion
	}
	job := models.JobSpec{
		Version:     version,
		Name:        strings.TrimSpace(conf.Name),
		Owner:       conf.Owner,
		Description: conf.Description,
		Labels:      labels,
		Schedule: models.JobSpecSchedule{
			StartDate: startDate,
			EndDate:   endDate,
			Interval:  conf.Schedule.Interval,
		},
		Behavior: models.JobSpecBehavior{
			CatchUp:       conf.Behavior.Catchup,
			DependsOnPast: conf.Behavior.DependsOnPast,
			Retry: models.JobSpecBehaviorRetry{
				Count:              conf.Behavior.Retry.Count,
				Delay:              retryDelayDuration,
				ExponentialBackoff: conf.Behavior.Retry.ExponentialBackoff,
			},
			Notify: jobNotifiers,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConf,
			Window: window,
		},
		Assets:       models.JobAssets{}.FromMap(conf.Asset),
		Dependencies: dependencies,
		Hooks:        hooks,
		Metadata: models.JobSpecMetadata{
			Resource: models.JobSpecResource{
				Request: models.JobSpecResourceConfig{
					Memory: conf.Metadata.Resource.Request.Memory,
					CPU:    conf.Metadata.Resource.Request.CPU,
				},
				Limit: models.JobSpecResourceConfig{
					Memory: conf.Metadata.Resource.Limit.Memory,
					CPU:    conf.Metadata.Resource.Limit.CPU,
				},
			},
			Airflow: models.JobSpecAirflow{
				Pool:  conf.Metadata.Airflow.Pool,
				Queue: conf.Metadata.Airflow.Queue,
			},
		},
		ExternalDependencies: externalDependency,
	}
	return job, nil
}

func (JobSpecAdapter) FromSpec(spec models.JobSpec) (JobSpec, error) {
	if spec.Task.Unit == nil {
		return JobSpec{}, errors.New("exec unit is nil")
	}

	labels := map[string]string{}
	for k, v := range spec.Labels {
		labels[k] = v
	}

	taskConf := map[string]string{}
	for _, l := range spec.Task.Config {
		taskConf[l.Name] = l.Value
	}

	retryDelayDuration := time.Duration(0)
	if spec.Behavior.Retry.Delay.Nanoseconds() > 0 {
		retryDelayDuration = spec.Behavior.Retry.Delay
	}

	var notifiers []JobSpecBehaviorNotifier
	for _, notify := range spec.Behavior.Notify {
		notifiers = append(notifiers, JobSpecBehaviorNotifier{
			On:       string(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	var truncateTo, offset, size string
	if spec.Task.Window != nil {
		truncateTo = spec.Task.Window.GetTruncateTo()
		offset = spec.Task.Window.GetOffset()
		size = spec.Task.Window.GetSize()
	}

	version := spec.Version
	if version == 0 {
		version = models.JobSpecDefaultVersion
	}
	parsed := JobSpec{
		Version:     version,
		Name:        spec.Name,
		Owner:       spec.Owner,
		Description: spec.Description,
		Labels:      labels,
		Schedule: JobSpecSchedule{
			Interval:  spec.Schedule.Interval,
			StartDate: spec.Schedule.StartDate.Format(models.JobDatetimeLayout),
		},
		Behavior: JobSpecBehavior{
			DependsOnPast: spec.Behavior.DependsOnPast,
			Catchup:       spec.Behavior.CatchUp,
			Retry: &JobSpecBehaviorRetry{
				Count:              spec.Behavior.Retry.Count,
				Delay:              retryDelayDuration,
				ExponentialBackoff: spec.Behavior.Retry.ExponentialBackoff,
			},
			Notify: notifiers,
		},
		Task: JobSpecTask{
			Name:   spec.Task.Unit.Info().Name,
			Config: taskConf,
			Window: JobSpecTaskWindow{
				TruncateTo: truncateTo,
				Offset:     offset,
				Size:       size,
			},
		},
		Asset:        spec.Assets.ToMap(),
		Dependencies: []JobSpecDependency{},
		Hooks:        []JobSpecHook{},
		Metadata: &JobSpecMetadata{
			Resource: &JobSpecMetadataResource{
				Request: &JobSpecMetadataResourceConfig{
					Memory: spec.Metadata.Resource.Request.Memory,
					CPU:    spec.Metadata.Resource.Request.CPU,
				},
				Limit: &JobSpecMetadataResourceConfig{
					Memory: spec.Metadata.Resource.Limit.Memory,
					CPU:    spec.Metadata.Resource.Limit.CPU,
				},
			},
			Airflow: &JobSpecMetadataAirflow{
				Pool:  spec.Metadata.Airflow.Pool,
				Queue: spec.Metadata.Airflow.Queue,
			},
		},
	}

	if spec.Schedule.EndDate != nil {
		parsed.Schedule.EndDate = spec.Schedule.EndDate.Format(models.JobDatetimeLayout)
	}
	for name, dep := range spec.Dependencies {
		parsed.Dependencies = append(parsed.Dependencies, JobSpecDependency{
			JobName: name,
			Type:    dep.Type.String(),
		})
	}
	// external http dependencies
	for _, dep := range spec.ExternalDependencies.HTTPDependencies {
		httpDep := &JobSpecDependencyHTTP{
			Name:          dep.Name,
			RequestParams: dep.RequestParams,
			URL:           dep.URL,
			Headers:       dep.Headers,
		}
		parsed.Dependencies = append(parsed.Dependencies, JobSpecDependency{
			HTTP: httpDep,
		})
	}

	// prep hooks
	for _, hook := range spec.Hooks {
		h := JobSpecHook{}.FromSpec(hook)
		parsed.Hooks = append(parsed.Hooks, h)
	}

	return parsed, nil
}

func NewJobSpecAdapter(pluginRepo models.PluginRepository) *JobSpecAdapter {
	return &JobSpecAdapter{
		pluginRepo: pluginRepo,
	}
}

func JobSpecConfigToMap(conf models.JobSpecConfigs) map[string]string {
	conv := map[string]string{}
	for _, c := range conf {
		conv[c.Name] = c.Value
	}
	return conv
}

func JobSpecConfigFromMap(conf map[string]string) models.JobSpecConfigs {
	conv := models.JobSpecConfigs{}
	for key, value := range conf {
		conv = append(conv, models.JobSpecConfigItem{
			Name:  key,
			Value: value,
		})
	}
	return conv
}

func prepHTTPDependency(dep JobSpecDependencyHTTP, index int) (models.HTTPDependency, error) {
	var httpDep models.HTTPDependency
	if _, err := url.ParseRequestURI(dep.URL); err != nil {
		return httpDep, fmt.Errorf("invalid url present on HTTPDependencies index %d of jobs.yaml, invalid reason : %w", index, err)
	}
	if dep.Name == "" {
		return httpDep, fmt.Errorf("empty name present on HTTPDependencies index %d of jobs.yaml", index)
	}
	httpDep = models.HTTPDependency(dep)
	return httpDep, nil
}
