package model

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/odpf/optimus/internal/utils"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobSpec struct {
	Version      int                 `yaml:"version,omitempty"`
	Name         string              `yaml:"name"`
	Owner        string              `yaml:"owner"`
	Description  string              `yaml:"description,omitempty"`
	Schedule     JobSpecSchedule     `yaml:"schedule"`
	Behavior     JobSpecBehavior     `yaml:"behavior"`
	Task         JobSpecTask         `yaml:"task"`
	Asset        map[string]string   `yaml:"-"`
	Labels       map[string]string   `yaml:"labels,omitempty"`
	Hooks        []JobSpecHook       `yaml:"hooks"`
	Dependencies []JobSpecDependency `yaml:"dependencies"`
	Metadata     *JobSpecMetadata    `yaml:"metadata,omitempty"`
}

type JobSpecSchedule struct {
	StartDate string `yaml:"start_date"`
	EndDate   string `yaml:"end_date,omitempty"`
	Interval  string `yaml:"interval"`
}

type JobSpecBehavior struct {
	DependsOnPast bool                      `yaml:"depends_on_past"`
	Catchup       bool                      `yaml:"catch_up"`
	Retry         *JobSpecBehaviorRetry     `yaml:"retry,omitempty"`
	Notify        []JobSpecBehaviorNotifier `yaml:"notify,omitempty"`
}

type JobSpecBehaviorRetry struct {
	Count              int           `yaml:"count,omitempty"`
	Delay              time.Duration `yaml:"delay,omitempty"`
	ExponentialBackoff bool          `yaml:"exponential_backoff,omitempty"`
}

type JobSpecBehaviorNotifier struct {
	On       string            `yaml:"on"`
	Config   map[string]string `yaml:"config"`
	Channels []string          `yaml:"channels"`
}

type JobSpecTask struct {
	Name   string            `yaml:"name"`
	Config map[string]string `yaml:"config,omitempty"`
	Window JobSpecTaskWindow `yaml:"window"`
}

type JobSpecTaskWindow struct {
	Size       string `yaml:"size"`
	Offset     string `yaml:"offset"`
	TruncateTo string `yaml:"truncate_to"`
}

type JobSpecHook struct {
	Name   string            `yaml:"name"`
	Config map[string]string `yaml:"config,omitempty"`
}

type JobSpecDependency struct {
	JobName string                 `yaml:"job,omitempty"`
	Type    string                 `yaml:"type,omitempty"`
	HTTP    *JobSpecDependencyHTTP `yaml:"http,omitempty"`
}

type JobSpecDependencyHTTP struct {
	Name          string            `yaml:"name"`
	RequestParams map[string]string `yaml:"params,omitempty"`
	URL           string            `yaml:"url"`
	Headers       map[string]string `yaml:"headers,omitempty"`
}

type JobSpecMetadata struct {
	Resource *JobSpecMetadataResource `yaml:"resource,omitempty"`
	Airflow  *JobSpecMetadataAirflow  `yaml:"airflow,omitempty"`
}

type JobSpecMetadataResource struct {
	Request *JobSpecMetadataResourceConfig `yaml:"request,omitempty"`
	Limit   *JobSpecMetadataResourceConfig `yaml:"limit,omitempty"`
}

type JobSpecMetadataResourceConfig struct {
	Memory string `yaml:"memory,omitempty"`
	CPU    string `yaml:"cpu,omitempty"`
}

type JobSpecMetadataAirflow struct {
	Pool  string `yaml:"pool" json:"pool"`
	Queue string `yaml:"queue" json:"queue"`
}

func (j JobSpec) ToProto() *pb.JobSpecification {
	return &pb.JobSpecification{
		Version:          int32(j.Version),
		Name:             j.Name,
		Owner:            j.Owner,
		StartDate:        j.Schedule.StartDate,
		EndDate:          j.Schedule.EndDate,
		Interval:         j.Schedule.Interval,
		DependsOnPast:    j.Behavior.DependsOnPast,
		CatchUp:          j.Behavior.Catchup,
		TaskName:         j.Task.Name,
		Config:           j.getProtoJobConfigItems(),
		WindowSize:       j.Task.Window.Size,
		WindowOffset:     j.Task.Window.Offset,
		WindowTruncateTo: j.Task.Window.TruncateTo,
		Dependencies:     j.getProtoJobDependencies(),
		Assets:           j.Asset,
		Hooks:            j.getProtoJobSpecHooks(),
		Description:      j.Description,
		Labels:           j.Labels,
		Behavior:         j.getProtoJobSpecBehavior(),
		Metadata:         j.getProtoJobMetadata(),
	}
}

func (j JobSpec) getProtoJobMetadata() *pb.JobMetadata {
	if j.Metadata == nil {
		return nil
	}

	var resource *pb.JobSpecMetadataResource
	if j.Metadata.Resource != nil {
		resource = &pb.JobSpecMetadataResource{
			Request: j.getProtoJobSpecMetadataResourceConfig(j.Metadata.Resource.Request),
			Limit:   j.getProtoJobSpecMetadataResourceConfig(j.Metadata.Resource.Limit),
		}
	}
	var airflow *pb.JobSpecMetadataAirflow
	if j.Metadata.Airflow != nil {
		airflow = &pb.JobSpecMetadataAirflow{
			Pool:  j.Metadata.Airflow.Pool,
			Queue: j.Metadata.Airflow.Queue,
		}
	}
	return &pb.JobMetadata{
		Resource: resource,
		Airflow:  airflow,
	}
}

func (j JobSpec) getProtoJobSpecMetadataResourceConfig(jobSpecMetadataResourceConfig *JobSpecMetadataResourceConfig) *pb.JobSpecMetadataResourceConfig {
	if jobSpecMetadataResourceConfig == nil {
		return nil
	}
	return &pb.JobSpecMetadataResourceConfig{
		Cpu:    jobSpecMetadataResourceConfig.CPU,
		Memory: jobSpecMetadataResourceConfig.Memory,
	}
}

func (j JobSpec) getProtoJobSpecBehavior() *pb.JobSpecification_Behavior {
	if j.Behavior.Retry == nil && len(j.Behavior.Notify) == 0 {
		return nil
	}

	var retry *pb.JobSpecification_Behavior_Retry
	if j.Behavior.Retry != nil {
		retry = &pb.JobSpecification_Behavior_Retry{
			Count:              int32(j.Behavior.Retry.Count),
			ExponentialBackoff: j.Behavior.Retry.ExponentialBackoff,
		}
		if j.Behavior.Retry.Delay != 0 {
			retry.Delay = durationpb.New(j.Behavior.Retry.Delay)
		}
	}
	var notifies []*pb.JobSpecification_Behavior_Notifiers
	if len(j.Behavior.Notify) > 0 {
		notifies = make([]*pb.JobSpecification_Behavior_Notifiers, len(j.Behavior.Notify))
		for i, notify := range j.Behavior.Notify {
			notifies[i] = &pb.JobSpecification_Behavior_Notifiers{
				On:       pb.JobEvent_Type(pb.JobEvent_Type_value[utils.ToEnumProto(string(notify.On), "type")]),
				Channels: notify.Channels,
				Config:   notify.Config,
			}
		}
	}
	return &pb.JobSpecification_Behavior{
		Retry:  retry,
		Notify: notifies,
	}
}

func (j JobSpec) getProtoJobSpecHooks() []*pb.JobSpecHook {
	protoJobSpecHooks := make([]*pb.JobSpecHook, len(j.Hooks))
	for i, hook := range j.Hooks {
		var protoJobConfigItems []*pb.JobConfigItem
		for name, value := range hook.Config {
			protoJobConfigItems = append(protoJobConfigItems, &pb.JobConfigItem{
				Name:  name,
				Value: value,
			})
		}
		protoJobSpecHooks[i] = &pb.JobSpecHook{
			Name:   hook.Name,
			Config: protoJobConfigItems,
		}
	}
	return protoJobSpecHooks
}

func (j JobSpec) getProtoJobDependencies() []*pb.JobDependency {
	protoJobDependencies := make([]*pb.JobDependency, len(j.Dependencies))
	for i, dependency := range j.Dependencies {
		jobSpecDependencyProto := pb.JobDependency{
			Name: dependency.JobName,
			Type: dependency.Type,
		}
		if dependency.HTTP != nil {
			jobSpecDependencyProto.HttpDependency = &pb.HttpDependency{
				Name:    dependency.HTTP.Name,
				Url:     dependency.HTTP.URL,
				Headers: dependency.HTTP.Headers,
				Params:  dependency.HTTP.RequestParams,
			}
		}
		protoJobDependencies[i] = &jobSpecDependencyProto
	}
	return protoJobDependencies
}

func (j JobSpec) getProtoJobConfigItems() []*pb.JobConfigItem {
	var protoJobConfigItems []*pb.JobConfigItem
	for name, value := range j.Task.Config {
		protoJobConfigItems = append(protoJobConfigItems, &pb.JobConfigItem{
			Name:  name, // TODO: on server, convert name to upper case
			Value: value,
		})
	}
	return protoJobConfigItems
}

// TODO: there are some refactors required, however it will be addressed once we relook at the job spec inheritence
func (j *JobSpec) MergeFrom(anotherJobSpec JobSpec) {
	j.Version = getValue(j.Version, anotherJobSpec.Version)
	j.Description = getValue(j.Description, anotherJobSpec.Description)
	j.Owner = getValue(j.Owner, anotherJobSpec.Owner)
	j.Schedule.Interval = getValue(j.Schedule.Interval, anotherJobSpec.Schedule.Interval)
	j.Schedule.StartDate = getValue(j.Schedule.StartDate, anotherJobSpec.Schedule.StartDate)
	j.Schedule.EndDate = getValue(j.Schedule.EndDate, anotherJobSpec.Schedule.EndDate)
	if j.Behavior.Retry != nil {
		j.Behavior.Retry.ExponentialBackoff = getValue(j.Behavior.Retry.ExponentialBackoff, anotherJobSpec.Behavior.Retry.ExponentialBackoff)
		j.Behavior.Retry.Delay = getValue(j.Behavior.Retry.Delay, anotherJobSpec.Behavior.Retry.Delay)
		j.Behavior.Retry.Count = getValue(j.Behavior.Retry.Count, anotherJobSpec.Behavior.Retry.Count)
	}
	j.Behavior.DependsOnPast = getValue(j.Behavior.DependsOnPast, anotherJobSpec.Behavior.DependsOnPast)
	j.Behavior.Catchup = getValue(j.Behavior.Catchup, anotherJobSpec.Behavior.Catchup)

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
			j.Dependencies = []JobSpecDependency{}
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

	j.Task.Name = getValue(j.Task.Name, anotherJobSpec.Task.Name)
	j.Task.Window.TruncateTo = getValue(j.Task.Window.TruncateTo, anotherJobSpec.Task.Window.TruncateTo)
	j.Task.Window.Offset = getValue(j.Task.Window.Offset, anotherJobSpec.Task.Window.Offset)
	j.Task.Window.Size = getValue(j.Task.Window.Size, anotherJobSpec.Task.Window.Size)
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
			j.Hooks = []JobSpecHook{}
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
			j.Hooks = append(j.Hooks, JobSpecHook{
				Name:   ph.Name,
				Config: ph.Config,
			})
		}
	}

	if metadata := anotherJobSpec.Metadata; metadata != nil {
		if resource := metadata.Resource; resource != nil {
			if request := resource.Request; request != nil {
				j.Metadata.Resource.Request.CPU = getValue(j.Metadata.Resource.Request.CPU, request.CPU)
				j.Metadata.Resource.Request.Memory = getValue(j.Metadata.Resource.Request.Memory, request.Memory)
			}
			if limit := resource.Limit; limit != nil {
				j.Metadata.Resource.Limit.CPU = getValue(j.Metadata.Resource.Limit.CPU, limit.CPU)
				j.Metadata.Resource.Limit.Memory = getValue(j.Metadata.Resource.Limit.Memory, limit.Memory)
			}
		}
		if airflow := metadata.Airflow; airflow != nil {
			j.Metadata.Airflow.Pool = getValue(j.Metadata.Airflow.Pool, airflow.Pool)
			j.Metadata.Airflow.Queue = getValue(j.Metadata.Airflow.Queue, airflow.Queue)
		}
	}
}

func getValue[V int | string | bool | time.Duration](reference, other V) V {
	if reference == *new(V) {
		return other
	}
	return reference
}
