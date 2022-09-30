package local

import (
	"time"

	"github.com/odpf/optimus/internal/utils"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type JobSpec struct {
	Version      int               `yaml:"version,omitempty"`
	Name         string            `yaml:"name"`
	Owner        string            `yaml:"owner"`
	Description  string            `yaml:"description,omitempty"`
	Schedule     JobSchedule       `yaml:"schedule"`
	Behavior     JobBehavior       `yaml:"behavior"`
	Task         JobTask           `yaml:"task"`
	Asset        map[string]string `yaml:"-"`
	Labels       map[string]string `yaml:"labels,omitempty"`
	Dependencies []JobDependency   `yaml:"dependencies"`
	Hooks        []JobHook         `yaml:"hooks"`
	Metadata     *JobSpecMetadata  `yaml:"metadata,omitempty"`
}

type JobSchedule struct {
	StartDate string `yaml:"start_date"`
	EndDate   string `yaml:"end_date,omitempty"`
	Interval  string `yaml:"interval"`
}

type JobBehavior struct {
	DependsOnPast bool              `yaml:"depends_on_past"`
	Catchup       bool              `yaml:"catch_up"`
	Retry         *JobBehaviorRetry `yaml:"retry,omitempty"`
	Notify        []JobNotifier     `yaml:"notify,omitempty"`
}

type JobBehaviorRetry struct {
	Count              int           `yaml:"count,omitempty"`
	Delay              time.Duration `yaml:"delay,omitempty"`
	ExponentialBackoff bool          `yaml:"exponential_backoff,omitempty"`
}

type JobNotifier struct {
	On       string            `yaml:"on"`
	Config   map[string]string `yaml:"config"`
	Channels []string          `yaml:"channels"`
}

type JobTask struct {
	Name   string            `yaml:"name"`
	Config map[string]string `yaml:"config,omitempty"`
	Window JobTaskWindow     `yaml:"window"`
}

type JobTaskWindow struct {
	Size       string `yaml:"size"`
	Offset     string `yaml:"offset"`
	TruncateTo string `yaml:"truncate_to"`
}

type JobHook struct {
	Name   string            `yaml:"name"`
	Config map[string]string `yaml:"config,omitempty"`
}

type JobDependency struct {
	JobName string          `yaml:"job,omitempty"`
	Type    string          `yaml:"type,omitempty"`
	HTTPDep *HTTPDependency `yaml:"http,omitempty"`
}

type HTTPDependency struct {
	Name          string            `yaml:"name"`
	RequestParams map[string]string `yaml:"params,omitempty"`
	URL           string            `yaml:"url"`
	Headers       map[string]string `yaml:"headers,omitempty"`
}

type JobSpecMetadata struct {
	Resource *JobSpecResource `yaml:"resource,omitempty"`
	Airflow  *JobSpecAirflow  `yaml:"airflow,omitempty"`
}

type JobSpecResource struct {
	Request *JobSpecResourceConfig `yaml:"request,omitempty"`
	Limit   *JobSpecResourceConfig `yaml:"limit,omitempty"`
}

type JobSpecResourceConfig struct {
	Memory string `yaml:"memory,omitempty"`
	CPU    string `yaml:"cpu,omitempty"`
}

type JobSpecAirflow struct {
	Pool  string `yaml:"pool" json:"pool"`
	Queue string `yaml:"queue" json:"queue"`
}

func (j *JobSpec) ToProto() *pb.JobSpecification {
	jobSpecProto := pb.JobSpecification{}

	jobSpecProto.Version = int32(j.Version)
	jobSpecProto.Name = j.Name
	jobSpecProto.Owner = j.Owner
	jobSpecProto.Description = j.Description
	jobSpecProto.Assets = j.Asset
	jobSpecProto.Labels = j.Labels

	// JobSchedule
	jobSpecProto.StartDate = j.Schedule.StartDate
	jobSpecProto.EndDate = j.Schedule.EndDate
	jobSpecProto.Interval = j.Schedule.Interval

	// JobBehaviour
	jobSpecProto.DependsOnPast = j.Behavior.DependsOnPast
	jobSpecProto.CatchUp = j.Behavior.Catchup
	jobSpecProto.Behavior = getJobSpecBehaviorProto(j.Behavior)

	// JobTask
	jobSpecProto.TaskName = j.Task.Name
	jobSpecProto.WindowSize = j.Task.Window.Size
	jobSpecProto.WindowOffset = j.Task.Window.Offset
	jobSpecProto.WindowTruncateTo = j.Task.Window.TruncateTo
	jobSpecProto.Config = getJobSpecConfigItemsProto(j.Task.Config)

	// Dependencies
	jobSpecProto.Dependencies = getJobSpecDependenciesProto(j.Dependencies)

	// Hooks
	jobSpecProto.Hooks = getJobSpecHooksProto(j.Hooks)

	// Metadata
	jobSpecProto.Metadata = getJobSpecMetadataProto(j.Metadata)

	return &jobSpecProto
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

	if j.Behavior.Retry != nil && j.Behavior.Retry.ExponentialBackoff == false {
		j.Behavior.Retry.ExponentialBackoff = anotherJobSpec.Behavior.Retry.ExponentialBackoff
	}
	if j.Behavior.Retry != nil && j.Behavior.Retry.Delay == 0 {
		j.Behavior.Retry.Delay = anotherJobSpec.Behavior.Retry.Delay
	}
	if j.Behavior.Retry != nil && j.Behavior.Retry.Count == 0 {
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
			j.Task.Config = map[string]string{}
		}
	}
	for pcKey, pc := range anotherJobSpec.Task.Config {
		alreadyExists := false
		for ccKey := range j.Task.Config {
			if ccKey == pcKey {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			j.Task.Config[pcKey] = pc
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
				for phcKey, phc := range ph.Config {
					alreadyExists := false
					for chciKey := range j.Hooks[chi].Config {
						if phcKey == chciKey {
							alreadyExists = true
							break
						}
					}
					if !alreadyExists {
						j.Hooks[chi].Config[phcKey] = phc
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
				Config: ph.Config,
			})
		}
	}
	if anotherJobSpec.Metadata != nil && anotherJobSpec.Metadata.Resource.Request.CPU != "" {
		j.Metadata.Resource.Request.CPU = anotherJobSpec.Metadata.Resource.Request.CPU
	}
	if anotherJobSpec.Metadata != nil && anotherJobSpec.Metadata.Resource.Request.Memory != "" {
		j.Metadata.Resource.Request.Memory = anotherJobSpec.Metadata.Resource.Request.Memory
	}
	if anotherJobSpec.Metadata != nil && anotherJobSpec.Metadata.Resource.Limit.CPU != "" {
		j.Metadata.Resource.Limit.CPU = anotherJobSpec.Metadata.Resource.Limit.CPU
	}
	if anotherJobSpec.Metadata != nil && anotherJobSpec.Metadata.Resource.Limit.Memory != "" {
		j.Metadata.Resource.Limit.Memory = anotherJobSpec.Metadata.Resource.Limit.Memory
	}
	if anotherJobSpec.Metadata != nil && anotherJobSpec.Metadata.Airflow.Pool != "" {
		j.Metadata.Airflow.Pool = anotherJobSpec.Metadata.Airflow.Pool
	}
	if anotherJobSpec.Metadata != nil && anotherJobSpec.Metadata.Airflow.Queue != "" {
		j.Metadata.Airflow.Queue = anotherJobSpec.Metadata.Airflow.Queue
	}
}

func getJobSpecBehaviorProto(jobSpecBehavior JobBehavior) *pb.JobSpecification_Behavior {
	if jobSpecBehavior.Retry == nil && len(jobSpecBehavior.Notify) == 0 {
		return nil
	}

	behavior := &pb.JobSpecification_Behavior{}
	// behavior retry
	if jobSpecBehavior.Retry != nil {
		behavior.Retry = &pb.JobSpecification_Behavior_Retry{
			Count:              int32(jobSpecBehavior.Retry.Count),
			ExponentialBackoff: jobSpecBehavior.Retry.ExponentialBackoff,
		}
		if jobSpecBehavior.Retry.Delay != 0 {
			behavior.Retry.Delay = durationpb.New(jobSpecBehavior.Retry.Delay)
		}
	}
	// behavior notify
	if len(jobSpecBehavior.Notify) > 0 {
		behavior.Notify = []*pb.JobSpecification_Behavior_Notifiers{}
		for _, notify := range jobSpecBehavior.Notify {
			behavior.Notify = append(behavior.Notify, &pb.JobSpecification_Behavior_Notifiers{
				On:       pb.JobEvent_Type(pb.JobEvent_Type_value[utils.ToEnumProto(string(notify.On), "type")]),
				Channels: notify.Channels,
				Config:   notify.Config,
			})
		}
	}

	return behavior
}

func getJobSpecConfigItemsProto(jobSpecConfig map[string]string) []*pb.JobConfigItem {
	jobSpecConfigProto := []*pb.JobConfigItem{}
	for configName, configValue := range jobSpecConfig {
		jobSpecConfigProto = append(jobSpecConfigProto, &pb.JobConfigItem{
			Name:  configName,
			Value: configValue,
		})
	}
	return jobSpecConfigProto
}

func getJobSpecDependenciesProto(jobSpecDependencies []JobDependency) []*pb.JobDependency {
	jobSpecDependenciesProto := []*pb.JobDependency{}
	for _, dependency := range jobSpecDependencies {
		jobSpecDependencyProto := pb.JobDependency{
			Name: dependency.JobName,
			Type: dependency.Type,
		}
		if dependency.HTTPDep != nil {
			jobSpecDependencyProto.HttpDependency = &pb.HttpDependency{
				Name:    dependency.HTTPDep.Name,
				Url:     dependency.HTTPDep.URL,
				Headers: dependency.HTTPDep.Headers,
				Params:  dependency.HTTPDep.RequestParams,
			}
			jobSpecDependenciesProto = append(jobSpecDependenciesProto, &jobSpecDependencyProto)
		}
	}
	return jobSpecDependenciesProto
}

func getJobSpecHooksProto(jobSpecHooks []JobHook) []*pb.JobSpecHook {
	jobSpecHooksProto := []*pb.JobSpecHook{}
	for _, hook := range jobSpecHooks {
		jobSpecHookProto := &pb.JobSpecHook{Name: hook.Name}
		jobSpecHookProto.Config = []*pb.JobConfigItem{}
		for hookConfigName, hookConfigValue := range hook.Config {
			jobSpecHookProto.Config = append(jobSpecHookProto.Config, &pb.JobConfigItem{
				Name:  hookConfigName,
				Value: hookConfigValue,
			})
			jobSpecHooksProto = append(jobSpecHooksProto, jobSpecHookProto)
		}
	}
	return jobSpecHooksProto
}

func getJobSpecMetadataProto(jobSpecMetadata *JobSpecMetadata) *pb.JobMetadata {
	if jobSpecMetadata == nil {
		return nil
	}

	jobSpecMetadataProto := &pb.JobMetadata{}

	// Resource
	if jobSpecMetadata.Resource != nil {
		jobSpecMetadataProto.Resource = &pb.JobSpecMetadataResource{}
		jobSpecMetadataProto.Resource.Request = getJobSpecResourceConfigProto(jobSpecMetadata.Resource.Request)
		jobSpecMetadataProto.Resource.Limit = getJobSpecResourceConfigProto(jobSpecMetadata.Resource.Limit)
	}

	// Airflow
	if jobSpecMetadata.Airflow != nil {
		jobSpecMetadataProto.Airflow = &pb.JobSpecMetadataAirflow{
			Pool:  jobSpecMetadata.Airflow.Pool,
			Queue: jobSpecMetadata.Airflow.Queue,
		}
	}

	return jobSpecMetadataProto
}

func getJobSpecResourceConfigProto(jobSpecResourceConfig *JobSpecResourceConfig) *pb.JobSpecMetadataResourceConfig {
	if jobSpecResourceConfig == nil {
		return nil
	}
	return &pb.JobSpecMetadataResourceConfig{
		Cpu:    jobSpecResourceConfig.CPU,
		Memory: jobSpecResourceConfig.Memory,
	}
}
