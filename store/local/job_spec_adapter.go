package local

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
	"gopkg.in/validator.v2"
)

const (
	JobConfigVersion = 1
	HoursInMonth     = time.Duration(30) * 24 * time.Hour
)

var (
	monthExp             = regexp.MustCompile("(\\+|-)?([0-9]+)(M)")
	ErrNotAMonthDuration = errors.New("invalid month string")
)

func init() {
	_ = validator.SetValidationFunc("isCron", utils.CronIntervalValidator)
}

// Job are inputs from user to create a job
// yaml representation of the job
type Job struct {
	Version      int    `yaml:"version,omitempty" validate:"min=1,max=100"`
	Name         string `validate:"min=3,max=1024"`
	Owner        string `yaml:"owner" validate:"min=3,max=1024"`
	Description  string `yaml:"description,omitempty"`
	Schedule     JobSchedule
	Behavior     JobBehavior
	Task         JobTask
	Asset        map[string]string `yaml:"asset,omitempty"`
	Labels       map[string]string `yaml:"labels,omitempty"`
	Dependencies []JobDependency
	Hooks        []JobHook
	Metadata     JobSpecMetadata `yaml:"metadata,omitempty"`
}

type JobSchedule struct {
	StartDate string `yaml:"start_date" json:"start_date" validate:"regexp=^\\d{4}-\\d{2}-\\d{2}$"`
	EndDate   string `yaml:"end_date,omitempty" json:"end_date"`
	Interval  string `yaml:"interval" validate:"isCron"`
}

type JobBehavior struct {
	DependsOnPast bool             `yaml:"depends_on_past" json:"depends_on_past"`
	Catchup       bool             `yaml:"catch_up" json:"catch_up"`
	Retry         JobBehaviorRetry `yaml:"retry,omitempty" json:"retry"`
	Notify        []JobNotifier    `yaml:"notify,omitempty" json:"notify"`
}

type JobBehaviorRetry struct {
	Count              int    `yaml:"count,omitempty" json:"count,omitempty"`
	Delay              string `yaml:"delay,omitempty" json:"delay,omitempty"`
	ExponentialBackoff bool   `yaml:"exponential_backoff,omitempty" json:"exponential_backoff,omitempty"`
}

type JobNotifier struct {
	On       string `yaml:"on" json:"on" validate:"regexp=^(sla_miss|failure|)$"`
	Config   map[string]string
	Channels []string
}

type JobTask struct {
	Name   string
	Config yaml.MapSlice `yaml:"config,omitempty"`
	Window JobTaskWindow
}

type JobTaskWindow struct {
	Size       string
	Offset     string
	TruncateTo string `yaml:"truncate_to" validate:"regexp=^(h|d|w|M|m)$"`
}

type JobHook struct {
	Name   string
	Config yaml.MapSlice `yaml:"config,omitempty"`
}

// ToSpec converts the local's JobHook representation to the optimus' models.JobSpecHook
func (a JobHook) ToSpec(pluginsRepo models.PluginRepository) (models.JobSpecHook, error) {
	hookUnit, err := pluginsRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, fmt.Errorf("spec reading error: %w", err)
	}
	return models.JobSpecHook{
		Config: JobSpecConfigFromYamlSlice(a.Config),
		Unit:   hookUnit,
	}, nil
}

// FromSpec converts the optimus' models.JobSpecHook representation to the local's JobHook
func (a JobHook) FromSpec(spec models.JobSpecHook) (JobHook, error) {
	return JobHook{
		Name:   spec.Unit.Info().Name,
		Config: JobSpecConfigToYamlSlice(spec.Config),
	}, nil
}

// JobSpecMetadata is a metadata representation for a job spec
type JobSpecMetadata struct {
	Resource JobSpecResource `yaml:"resource,omitempty"`
}

// JobSpecResource represents the resource management configuration
type JobSpecResource struct {
	Request JobSpecResourceConfig `yaml:"request,omitempty"`
	Limit   JobSpecResourceConfig `yaml:"limit,omitempty"`
}

// JobSpecResourceConfig is a resource configuration
type JobSpecResourceConfig struct {
	Memory string `yaml:"memory,omitempty"`
	CPU    string `yaml:"cpu,omitempty"`
}

// MergeFrom merges parent job into this
// - non zero values on child are ignored
// - zero values on parent are ignored
// - slices are merged
func (conf *Job) MergeFrom(parent Job) {
	if conf.Version == 0 {
		conf.Version = parent.Version
	}

	if conf.Schedule.Interval == "" {
		conf.Schedule.Interval = parent.Schedule.Interval
	}
	if conf.Schedule.StartDate == "" {
		conf.Schedule.StartDate = parent.Schedule.StartDate
	}
	if conf.Schedule.EndDate == "" {
		conf.Schedule.EndDate = parent.Schedule.EndDate
	}

	if conf.Behavior.Retry.ExponentialBackoff == false {
		conf.Behavior.Retry.ExponentialBackoff = parent.Behavior.Retry.ExponentialBackoff
	}
	if conf.Behavior.Retry.Delay == "" {
		conf.Behavior.Retry.Delay = parent.Behavior.Retry.Delay
	}
	if conf.Behavior.Retry.Count == 0 {
		conf.Behavior.Retry.Count = parent.Behavior.Retry.Count
	}
	if conf.Behavior.DependsOnPast == false {
		conf.Behavior.DependsOnPast = parent.Behavior.DependsOnPast
	}
	if conf.Behavior.Catchup == false {
		conf.Behavior.Catchup = parent.Behavior.Catchup
	}
	for _, pNotify := range parent.Behavior.Notify {
		childNotifyIdx := -1
		for cnIdx, cn := range conf.Behavior.Notify {
			if pNotify.On == cn.On {
				childNotifyIdx = cnIdx
				break
			}
		}
		if childNotifyIdx == -1 {
			conf.Behavior.Notify = append(conf.Behavior.Notify, pNotify)
		} else {
			// already exists just inherit

			// configs
			if conf.Behavior.Notify[childNotifyIdx].Config == nil {
				conf.Behavior.Notify[childNotifyIdx].Config = map[string]string{}
			}
			for pNotifyConfigKey, pNotifyConfigVal := range pNotify.Config {
				if _, ok := conf.Behavior.Notify[childNotifyIdx].Config[pNotifyConfigKey]; !ok {
					conf.Behavior.Notify[childNotifyIdx].Config[pNotifyConfigKey] = pNotifyConfigVal
				}
			}

			// channels
			if conf.Behavior.Notify[childNotifyIdx].Channels == nil {
				conf.Behavior.Notify[childNotifyIdx].Channels = []string{}
			}
			for _, pNotifyChannel := range pNotify.Channels {
				childNotifyChannelIdx := -1
				for cnChannelIdx, cnChannel := range conf.Behavior.Notify[childNotifyIdx].Channels {
					if cnChannel == pNotifyChannel {
						childNotifyChannelIdx = cnChannelIdx
						break
					}
				}
				if childNotifyChannelIdx == -1 {
					conf.Behavior.Notify[childNotifyIdx].Channels = append(conf.Behavior.Notify[childNotifyIdx].Channels, pNotifyChannel)
				}
			}
		}
	}

	if conf.Description == "" {
		conf.Description = parent.Description
	}

	if conf.Owner == "" {
		conf.Owner = parent.Owner
	}

	if parent.Labels != nil {
		if conf.Labels == nil {
			conf.Labels = map[string]string{}
		}
	}
	for k, v := range parent.Labels {
		if _, ok := conf.Labels[k]; !ok {
			conf.Labels[k] = v
		}
	}

	if parent.Dependencies != nil {
		if conf.Dependencies == nil {
			conf.Dependencies = []JobDependency{}
		}
	}
	for _, dep := range parent.Dependencies {
		alreadyExists := false
		for _, cd := range conf.Dependencies {
			if dep.JobName == cd.JobName && dep.Type == cd.Type {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			conf.Dependencies = append(conf.Dependencies, dep)
		}
	}

	if conf.Task.Name == "" {
		conf.Task.Name = parent.Task.Name
	}
	if conf.Task.Window.TruncateTo == "" {
		conf.Task.Window.TruncateTo = parent.Task.Window.TruncateTo
	}
	if conf.Task.Window.Offset == "" {
		conf.Task.Window.Offset = parent.Task.Window.Offset
	}
	if conf.Task.Window.Size == "" {
		conf.Task.Window.Size = parent.Task.Window.Size
	}
	if parent.Task.Config != nil {
		if conf.Task.Config == nil {
			conf.Task.Config = []yaml.MapItem{}
		}
	}
	for _, pc := range parent.Task.Config {
		alreadyExists := false
		for _, cc := range conf.Task.Config {
			if cc.Key.(string) == pc.Key.(string) {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			conf.Task.Config = append(conf.Task.Config, pc)
		}
	}

	if parent.Hooks != nil {
		if conf.Hooks == nil {
			conf.Hooks = []JobHook{}
		}
	}
	existingHooks := map[string]bool{}
	for _, ph := range parent.Hooks {
		for chi := range conf.Hooks {
			existingHooks[conf.Hooks[chi].Name] = true
			// check if hook already present in child
			if ph.Name == conf.Hooks[chi].Name {
				// try to copy configs
				for _, phc := range ph.Config {
					alreadyExists := false
					for chci := range conf.Hooks[chi].Config {
						if phc.Key == conf.Hooks[chi].Config[chci].Key {
							alreadyExists = true
							break
						}
					}
					if !alreadyExists {
						conf.Hooks[chi].Config = append(conf.Hooks[chi].Config, phc)
					}
				}
			}
		}
	}
	for _, ph := range parent.Hooks {
		// copy non existing hooks
		if _, ok := existingHooks[ph.Name]; !ok {
			conf.Hooks = append(conf.Hooks, JobHook{
				Name:   ph.Name,
				Config: append(yaml.MapSlice{}, ph.Config...),
			})
		}
	}
	if parent.Metadata.Resource.Request.CPU != "" {
		conf.Metadata.Resource.Request.CPU = parent.Metadata.Resource.Request.CPU
	}
	if parent.Metadata.Resource.Request.Memory != "" {
		conf.Metadata.Resource.Request.Memory = parent.Metadata.Resource.Request.Memory
	}
	if parent.Metadata.Resource.Limit.CPU != "" {
		conf.Metadata.Resource.Limit.CPU = parent.Metadata.Resource.Limit.CPU
	}
	if parent.Metadata.Resource.Limit.Memory != "" {
		conf.Metadata.Resource.Limit.Memory = parent.Metadata.Resource.Limit.Memory
	}
}

func (conf *Job) prepareWindow() (models.JobSpecTaskWindow, error) {
	var err error
	window := models.JobSpecTaskWindow{}
	window.Size = time.Hour * 24
	window.Offset = 0
	window.TruncateTo = "d"

	if conf.Task.Window.TruncateTo != "" {
		window.TruncateTo = conf.Task.Window.TruncateTo
	}

	// check if string contains monthly notation
	if conf.Task.Window.Size != "" {
		window.Size, err = tryParsingInMonths(conf.Task.Window.Size)
		if err != nil {
			// treat as normal duration
			window.Size, err = time.ParseDuration(conf.Task.Window.Size)
			if err != nil {
				return window, fmt.Errorf("failed to parse task window %s with size %v: %w", conf.Name, conf.Task.Window.Size, err)
			}
		}
	}

	// check if string contains monthly notation
	if conf.Task.Window.Offset != "" {
		window.Offset, err = tryParsingInMonths(conf.Task.Window.Offset)
		if err != nil {
			// treat as normal duration
			window.Offset, err = time.ParseDuration(conf.Task.Window.Offset)
			if err != nil {
				return window, fmt.Errorf("failed to parse task window %s with offset %v: %w", conf.Name, conf.Task.Window.Offset, err)
			}
		}
	}

	return window, nil
}

type JobDependency struct {
	JobName string         `yaml:"job,omitempty"`
	Type    string         `yaml:"type,omitempty"`
	HTTPDep HTTPDependency `yaml:"http,omitempty"`
}

type HTTPDependency struct {
	Name          string            `yaml:"name"`
	RequestParams map[string]string `yaml:"params,omitempty"`
	URL           string            `yaml:"url"`
	Headers       map[string]string `yaml:"headers,omitempty"`
}

type JobSpecAdapter struct {
	pluginRepo models.PluginRepository
}

func (adapt JobSpecAdapter) ToSpec(conf Job) (models.JobSpec, error) {
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
		if !reflect.DeepEqual(dep.HTTPDep, HTTPDependency{}) {
			httpDep, err := prepHTTPDependency(dep.HTTPDep, index)
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

	// prep window
	window, err := conf.prepareWindow()
	if err != nil {
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
	for _, c := range conf.Task.Config {
		taskConf = append(taskConf, models.JobSpecConfigItem{
			Name:  c.Key.(string),
			Value: c.Value.(string), // TODO: panics when value not valid, error with macros
		})
	}

	retryDelayDuration := time.Duration(0)
	if conf.Behavior.Retry.Delay != "" {
		retryDelayDuration, err = time.ParseDuration(conf.Behavior.Retry.Delay)
		if err != nil {
			return models.JobSpec{}, err
		}
	}

	var jobNotifiers []models.JobSpecNotifier
	for _, notify := range conf.Behavior.Notify {
		jobNotifiers = append(jobNotifiers, models.JobSpecNotifier{
			On:       models.JobEventType(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	job := models.JobSpec{
		Version:     conf.Version,
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
		},
		ExternalDependencies: externalDependency,
	}
	return job, nil
}

func (adapt JobSpecAdapter) FromSpec(spec models.JobSpec) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("exec unit is nil")
	}

	labels := map[string]string{}
	for k, v := range spec.Labels {
		labels[k] = v
	}

	taskConf := yaml.MapSlice{}
	for _, l := range spec.Task.Config {
		taskConf = append(taskConf, yaml.MapItem{
			Key:   l.Name,
			Value: l.Value,
		})
	}

	retryDelayDuration := ""
	if spec.Behavior.Retry.Delay.Nanoseconds() > 0 {
		retryDelayDuration = spec.Behavior.Retry.Delay.String()
	}

	var notifiers []JobNotifier
	for _, notify := range spec.Behavior.Notify {
		notifiers = append(notifiers, JobNotifier{
			On:       string(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	parsed := Job{
		Version:     spec.Version,
		Name:        spec.Name,
		Owner:       spec.Owner,
		Description: spec.Description,
		Labels:      labels,
		Schedule: JobSchedule{
			Interval:  spec.Schedule.Interval,
			StartDate: spec.Schedule.StartDate.Format(models.JobDatetimeLayout),
		},
		Behavior: JobBehavior{
			DependsOnPast: spec.Behavior.DependsOnPast,
			Catchup:       spec.Behavior.CatchUp,
			Retry: JobBehaviorRetry{
				Count:              spec.Behavior.Retry.Count,
				Delay:              retryDelayDuration,
				ExponentialBackoff: spec.Behavior.Retry.ExponentialBackoff,
			},
			Notify: notifiers,
		},
		Task: JobTask{
			Name:   spec.Task.Unit.Info().Name,
			Config: taskConf,
			Window: JobTaskWindow{
				Size:       spec.Task.Window.SizeString(),
				Offset:     spec.Task.Window.OffsetString(),
				TruncateTo: spec.Task.Window.TruncateTo,
			},
		},
		Asset:        spec.Assets.ToMap(),
		Dependencies: []JobDependency{},
		Hooks:        []JobHook{},
		Metadata: JobSpecMetadata{
			Resource: JobSpecResource{
				Request: JobSpecResourceConfig{
					Memory: spec.Metadata.Resource.Request.Memory,
					CPU:    spec.Metadata.Resource.Request.CPU,
				},
				Limit: JobSpecResourceConfig{
					Memory: spec.Metadata.Resource.Limit.Memory,
					CPU:    spec.Metadata.Resource.Limit.CPU,
				},
			},
		},
	}

	if spec.Schedule.EndDate != nil {
		parsed.Schedule.EndDate = spec.Schedule.EndDate.Format(models.JobDatetimeLayout)
	}
	for name, dep := range spec.Dependencies {
		parsed.Dependencies = append(parsed.Dependencies, JobDependency{
			JobName: name,
			Type:    dep.Type.String(),
		})
	}
	// external http dependencies
	for _, dep := range spec.ExternalDependencies.HTTPDependencies {
		parsed.Dependencies = append(parsed.Dependencies, JobDependency{
			HTTPDep: HTTPDependency(dep),
		})
	}

	// prep hooks
	for _, hook := range spec.Hooks {
		h, err := JobHook{}.FromSpec(hook)
		if err != nil {
			return parsed, err
		}
		parsed.Hooks = append(parsed.Hooks, h)
	}

	return parsed, nil
}

func NewJobSpecAdapter(pluginRepo models.PluginRepository) *JobSpecAdapter {
	return &JobSpecAdapter{
		pluginRepo: pluginRepo,
	}
}

func JobSpecConfigToYamlSlice(conf models.JobSpecConfigs) yaml.MapSlice {
	conv := yaml.MapSlice{}
	for _, c := range conf {
		conv = append(conv, yaml.MapItem{
			Key:   c.Name,
			Value: c.Value,
		})
	}
	return conv
}

func JobSpecConfigFromYamlSlice(conf yaml.MapSlice) models.JobSpecConfigs {
	conv := models.JobSpecConfigs{}
	for _, c := range conf {
		conv = append(conv, models.JobSpecConfigItem{
			Name:  c.Key.(string),
			Value: c.Value.(string),
		})
	}
	return conv
}

// check if string contains monthly notation
func tryParsingInMonths(str string) (time.Duration, error) {
	sz := time.Duration(0)
	monthMatches := monthExp.FindAllStringSubmatch(str, -1)
	if len(monthMatches) > 0 && len(monthMatches[0]) == 4 {
		// replace month notation with days first, treating 1M as 30 days
		monthsCount, err := strconv.Atoi(monthMatches[0][2])
		if err != nil {
			return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
		}
		sz = HoursInMonth * time.Duration(monthsCount)
		if monthMatches[0][1] == "-" {
			sz *= -1
		}

		str = strings.TrimSpace(monthExp.ReplaceAllString(str, ""))
		if len(str) > 0 {
			// check if there is remaining time that we can still parse
			remainingTime, err := time.ParseDuration(str)
			if err != nil {
				return sz, fmt.Errorf("failed to parse task configuration of %s: %w", str, err)
			}
			sz += remainingTime
		}
		return sz, nil
	}
	return sz, ErrNotAMonthDuration
}

func prepHTTPDependency(dep HTTPDependency, index int) (models.HTTPDependency, error) {
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
