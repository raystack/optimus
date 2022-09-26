package local

import "gopkg.in/yaml.v2"

type JobSpec struct {
	Version      int               `yaml:"version"`
	Name         string            `yaml:"name"`
	Owner        string            `yaml:"owner"`
	Description  string            `yaml:"description"`
	Schedule     JobSchedule       `yaml:"schedule"`
	Behavior     JobBehavior       `yaml:"behavior"`
	Task         JobTask           `yaml:"task"`
	Asset        map[string]string `yaml:"asset"`
	Labels       map[string]string `yaml:"labels"`
	Dependencies []JobDependency   `yaml:"dependencies"`
	Hooks        []JobHook         `yaml:"hooks"`
	Metadata     JobSpecMetadata   `yaml:"metadata"`
}

type JobSchedule struct {
	StartDate string `yaml:"start_date"`
	EndDate   string `yaml:"end_date"`
	Interval  string `yaml:"interval"`
}

type JobBehavior struct {
	DependsOnPast bool             `yaml:"depends_on_past"`
	Catchup       bool             `yaml:"catch_up"`
	Retry         JobBehaviorRetry `yaml:"retry"`
	Notify        []JobNotifier    `yaml:"notify"`
}

type JobBehaviorRetry struct {
	Count              int    `yaml:"count"`
	Delay              string `yaml:"delay"`
	ExponentialBackoff bool   `yaml:"exponential_backoff"`
}

type JobNotifier struct {
	On       string            `yaml:"on"`
	Config   map[string]string `yaml:"config"`
	Channels []string          `yaml:"channels"`
}

type JobTask struct {
	Name   string        `yaml:"name"`
	Config yaml.MapSlice `yaml:"config"`
	Window JobTaskWindow `yaml:"window"`
}

type JobTaskWindow struct {
	Size       string `yaml:"size"`
	Offset     string `yaml:"offset"`
	TruncateTo string `yaml:"truncate_to"`
}

type JobHook struct {
	Name   string        `yaml:"name"`
	Config yaml.MapSlice `yaml:"config"`
}

type JobDependency struct {
	JobName string         `yaml:"job"`
	Type    string         `yaml:"type"`
	HTTPDep HTTPDependency `yaml:"http"`
}

type HTTPDependency struct {
	Name          string            `yaml:"name"`
	RequestParams map[string]string `yaml:"params"`
	URL           string            `yaml:"url"`
	Headers       map[string]string `yaml:"headers"`
}

type JobSpecMetadata struct {
	Resource JobSpecResource `yaml:"resource"`
	Airflow  JobSpecAirflow  `yaml:"airflow"`
}

type JobSpecResource struct {
	Request JobSpecResourceConfig `yaml:"request"`
	Limit   JobSpecResourceConfig `yaml:"limit"`
}

type JobSpecResourceConfig struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
}

type JobSpecAirflow struct {
	Pool  string `yaml:"pool"`
	Queue string `yaml:"queue"`
}

// TODO: can use open source lib to merge the struct eg. https://github.com/imdario/mergo
// MergeFrom merges parent job into this
// - non zero values on child are ignored
// - zero values on parent are ignored
// - slices are merged
func (j *JobSpec) MergeFrom(anotherJobSpec JobSpec) {
	if j.Version == 0 {
		j.Version = anotherJobSpec.Version
	}

	if j.Schedule.Interval == "" {
		j.Schedule.Interval = anotherJobSpec.Schedule.Interval
	}
	if j.Schedule.StartDate == "" {
		j.Schedule.StartDate = anotherJobSpec.Schedule.StartDate
	}
	if j.Schedule.EndDate == "" {
		j.Schedule.EndDate = anotherJobSpec.Schedule.EndDate
	}

	if j.Behavior.Retry.ExponentialBackoff == false {
		j.Behavior.Retry.ExponentialBackoff = anotherJobSpec.Behavior.Retry.ExponentialBackoff
	}
	if j.Behavior.Retry.Delay == "" {
		j.Behavior.Retry.Delay = anotherJobSpec.Behavior.Retry.Delay
	}
	if j.Behavior.Retry.Count == 0 {
		j.Behavior.Retry.Count = anotherJobSpec.Behavior.Retry.Count
	}
	if j.Behavior.DependsOnPast == false {
		j.Behavior.DependsOnPast = anotherJobSpec.Behavior.DependsOnPast
	}
	if j.Behavior.Catchup == false {
		j.Behavior.Catchup = anotherJobSpec.Behavior.Catchup
	}
	for _, pNotify := range anotherJobSpec.Behavior.Notify {
		childNotifyIdx := -1
		for cnIdx, cn := range j.Behavior.Notify {
			if pNotify.On == cn.On {
				childNotifyIdx = cnIdx
				break
			}
		}
		if childNotifyIdx == -1 {
			j.Behavior.Notify = append(j.Behavior.Notify, pNotify)
		} else {
			// already exists just inherit

			// configs
			if j.Behavior.Notify[childNotifyIdx].Config == nil {
				j.Behavior.Notify[childNotifyIdx].Config = map[string]string{}
			}
			for pNotifyConfigKey, pNotifyConfigVal := range pNotify.Config {
				if _, ok := j.Behavior.Notify[childNotifyIdx].Config[pNotifyConfigKey]; !ok {
					j.Behavior.Notify[childNotifyIdx].Config[pNotifyConfigKey] = pNotifyConfigVal
				}
			}

			// channels
			if j.Behavior.Notify[childNotifyIdx].Channels == nil {
				j.Behavior.Notify[childNotifyIdx].Channels = []string{}
			}
			for _, pNotifyChannel := range pNotify.Channels {
				childNotifyChannelIdx := -1
				for cnChannelIdx, cnChannel := range j.Behavior.Notify[childNotifyIdx].Channels {
					if cnChannel == pNotifyChannel {
						childNotifyChannelIdx = cnChannelIdx
						break
					}
				}
				if childNotifyChannelIdx == -1 {
					j.Behavior.Notify[childNotifyIdx].Channels = append(j.Behavior.Notify[childNotifyIdx].Channels, pNotifyChannel)
				}
			}
		}
	}

	if j.Description == "" {
		j.Description = anotherJobSpec.Description
	}

	if j.Owner == "" {
		j.Owner = anotherJobSpec.Owner
	}

	if anotherJobSpec.Labels != nil {
		if j.Labels == nil {
			j.Labels = map[string]string{}
		}
	}
	for k, v := range anotherJobSpec.Labels {
		if _, ok := j.Labels[k]; !ok {
			j.Labels[k] = v
		}
	}

	if anotherJobSpec.Dependencies != nil {
		if j.Dependencies == nil {
			j.Dependencies = []JobDependency{}
		}
	}
	for _, dep := range anotherJobSpec.Dependencies {
		alreadyExists := false
		for _, cd := range j.Dependencies {
			if dep.JobName == cd.JobName && dep.Type == cd.Type {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			j.Dependencies = append(j.Dependencies, dep)
		}
	}

	if j.Task.Name == "" {
		j.Task.Name = anotherJobSpec.Task.Name
	}
	if j.Task.Window.TruncateTo == "" {
		j.Task.Window.TruncateTo = anotherJobSpec.Task.Window.TruncateTo
	}
	if j.Task.Window.Offset == "" {
		j.Task.Window.Offset = anotherJobSpec.Task.Window.Offset
	}
	if j.Task.Window.Size == "" {
		j.Task.Window.Size = anotherJobSpec.Task.Window.Size
	}
	if anotherJobSpec.Task.Config != nil {
		if j.Task.Config == nil {
			j.Task.Config = []yaml.MapItem{}
		}
	}
	for _, pc := range anotherJobSpec.Task.Config {
		alreadyExists := false
		for _, cc := range j.Task.Config {
			if cc.Key.(string) == pc.Key.(string) {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			j.Task.Config = append(j.Task.Config, pc)
		}
	}

	if anotherJobSpec.Hooks != nil {
		if j.Hooks == nil {
			j.Hooks = []JobHook{}
		}
	}
	existingHooks := map[string]bool{}
	for _, ph := range anotherJobSpec.Hooks {
		for chi := range j.Hooks {
			existingHooks[j.Hooks[chi].Name] = true
			// check if hook already present in child
			if ph.Name == j.Hooks[chi].Name {
				// try to copy configs
				for _, phc := range ph.Config {
					alreadyExists := false
					for chci := range j.Hooks[chi].Config {
						if phc.Key == j.Hooks[chi].Config[chci].Key {
							alreadyExists = true
							break
						}
					}
					if !alreadyExists {
						j.Hooks[chi].Config = append(j.Hooks[chi].Config, phc)
					}
				}
			}
		}
	}
	for _, ph := range anotherJobSpec.Hooks {
		// copy non existing hooks
		if _, ok := existingHooks[ph.Name]; !ok {
			j.Hooks = append(j.Hooks, JobHook{
				Name:   ph.Name,
				Config: append(yaml.MapSlice{}, ph.Config...),
			})
		}
	}
	if anotherJobSpec.Metadata.Resource.Request.CPU != "" {
		j.Metadata.Resource.Request.CPU = anotherJobSpec.Metadata.Resource.Request.CPU
	}
	if anotherJobSpec.Metadata.Resource.Request.Memory != "" {
		j.Metadata.Resource.Request.Memory = anotherJobSpec.Metadata.Resource.Request.Memory
	}
	if anotherJobSpec.Metadata.Resource.Limit.CPU != "" {
		j.Metadata.Resource.Limit.CPU = anotherJobSpec.Metadata.Resource.Limit.CPU
	}
	if anotherJobSpec.Metadata.Resource.Limit.Memory != "" {
		j.Metadata.Resource.Limit.Memory = anotherJobSpec.Metadata.Resource.Limit.Memory
	}
	if anotherJobSpec.Metadata.Airflow.Pool != "" {
		j.Metadata.Airflow.Pool = anotherJobSpec.Metadata.Airflow.Pool
	}
	if anotherJobSpec.Metadata.Airflow.Queue != "" {
		j.Metadata.Airflow.Queue = anotherJobSpec.Metadata.Airflow.Queue
	}
}
