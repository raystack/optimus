package v1beta1

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func fromJobProto(jobTenant tenant.Tenant, js *pb.JobSpecification) (*job.JobSpec, error) {
	var retry *job.Retry
	var alerts []*job.Alert
	if js.Behavior != nil {
		retry = toRetry(js.Behavior.Retry)
		alerts = toAlerts(js.Behavior.Notify)
	}

	schedule := job.NewSchedule(js.StartDate, js.EndDate, js.Interval, js.DependsOnPast, js.CatchUp, retry)

	window, err := models.NewWindow(int(js.Version), js.WindowTruncateTo, js.WindowOffset, js.WindowSize)
	if err != nil {
		return nil, err
	}
	if err := window.Validate(); err != nil {
		return nil, err
	}

	taskConfig := toConfig(js.Config)
	task := job.NewTask(js.TaskName, taskConfig)

	hooks := toHooks(js.Hooks)

	dependencies := toDependencies(js.Dependencies)

	metadata := toMetadata(js.Metadata)

	//TODO: try explore builder. too many arguments
	return job.NewJobSpec(jobTenant, int(js.Version), js.Name, js.Owner, js.Description, js.Labels,
		schedule, window, task, hooks, alerts, dependencies, js.Assets, metadata)
}

func toRetry(protoRetry *pb.JobSpecification_Behavior_Retry) *job.Retry {
	if protoRetry == nil {
		return nil
	}
	return job.NewRetry(int(protoRetry.Count), protoRetry.Delay.GetNanos(), protoRetry.ExponentialBackoff)
}

func toHooks(hooksProto []*pb.JobSpecHook) []*job.Hook {
	hooks := make([]*job.Hook, len(hooksProto))
	for i, hookProto := range hooksProto {
		hookConfig := toConfig(hookProto.Config)
		hooks[i] = job.NewHook(hookProto.Name, hookConfig)
	}
	return hooks
}

func toAlerts(notifiers []*pb.JobSpecification_Behavior_Notifiers) []*job.Alert {
	alerts := make([]*job.Alert, len(notifiers))
	for i, notify := range notifiers {
		alertOn := job.EventType(utils.FromEnumProto(notify.On.String(), "type"))
		alerts[i] = job.NewAlert(alertOn, notify.Channels, notify.Config)
	}
	return alerts
}

func toDependencies(dependenciesProto []*pb.JobDependency) *job.DependencySpec {
	var jobDependencies []string
	var httpDependencies []*job.HTTPDependency
	for _, dependency := range dependenciesProto {
		if dependency.HttpDependency == nil {
			jobDependencies = append(jobDependencies, dependency.Name)
			continue
		}
		httpDependencyProto := dependency.HttpDependency
		httpDependency := job.NewHTTPDependency(httpDependencyProto.Name, httpDependencyProto.Url, httpDependencyProto.Headers, httpDependencyProto.Params)
		httpDependencies = append(httpDependencies, httpDependency)
	}
	dependencies := job.NewDependencySpec(jobDependencies, httpDependencies)
	return dependencies
}

func toMetadata(jobMetadata *pb.JobMetadata) *job.Metadata {
	if jobMetadata == nil {
		return nil
	}

	var resourceMetadata *job.ResourceMetadata
	if jobMetadata.Resource != nil {
		metadataResourceProto := jobMetadata.Resource
		metadataResourceRequest := job.NewResourceConfig(metadataResourceProto.Request.Cpu, metadataResourceProto.Request.Memory)
		metadataResourceLimit := job.NewResourceConfig(metadataResourceProto.Limit.Cpu, metadataResourceProto.Limit.Memory)
		resourceMetadata = job.NewResourceMetadata(metadataResourceRequest, metadataResourceLimit)
	}

	schedulerMetadata := make(map[string]string)
	if jobMetadata.Airflow != nil {
		metadataSchedulerProto := jobMetadata.Airflow
		schedulerMetadata["pool"] = metadataSchedulerProto.Pool
		schedulerMetadata["queue"] = metadataSchedulerProto.Queue
	}
	metadata := job.NewMetadata(resourceMetadata, schedulerMetadata)
	return metadata
}

func toConfig(configs []*pb.JobConfigItem) *job.Config {
	configMap := make(map[string]string, len(configs))
	for _, config := range configs {
		configMap[config.Name] = config.Value
	}
	return job.NewConfig(configMap)
}
