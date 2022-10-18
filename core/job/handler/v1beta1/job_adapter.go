package v1beta1

import (
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/utils"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func fromJobProto(tnnt *tenant.WithDetails, js *pb.JobSpecification) (*dto.JobSpec, error) {
	retryProto := js.Behavior.Retry
	retry := dto.NewRetry(int(retryProto.Count), retryProto.Delay.GetNanos(), retryProto.ExponentialBackoff)

	schedule := dto.NewSchedule(js.StartDate, js.EndDate, js.Interval, js.DependsOnPast, js.CatchUp, retry)

	window := dto.NewWindow(js.WindowSize, js.WindowOffset, js.WindowTruncateTo)

	taskConfig := toConfig(js.Config)
	task := dto.NewTask(js.TaskName, taskConfig)

	hooks := toHooks(js.Hooks)

	alerts := toAlerts(js.Behavior)

	dependencies := toDependencies(js.Dependencies)

	metadata := toMetadata(js.Metadata)

	return dto.NewJobSpec(tnnt, int(js.Version), js.Name, js.Owner, js.Description, js.Labels,
		schedule, window, task, hooks, alerts, dependencies, js.Assets, metadata)
}

func toHooks(hooksProto []*pb.JobSpecHook) []*dto.Hook {
	hooks := make([]*dto.Hook, len(hooksProto))
	for _, hookProto := range hooksProto {
		hookConfig := toConfig(hookProto.Config)
		hooks = append(hooks, dto.NewHook(hookProto.Name, hookConfig))
	}
	return hooks
}

func toAlerts(behavior *pb.JobSpecification_Behavior) []*dto.Alert {
	alerts := make([]*dto.Alert, len(behavior.Notify))
	for _, notify := range behavior.Notify {
		alertOn := dto.EventType(utils.FromEnumProto(notify.On.String(), "type"))
		alerts = append(alerts, dto.NewAlert(alertOn, notify.Channels, notify.Config))
	}
	return alerts
}

func toDependencies(dependenciesProto []*pb.JobDependency) *dto.Dependencies {
	var jobDependencies []string
	var httpDependencies []*dto.HttpDependency
	for _, dependency := range dependenciesProto {
		if dependency.HttpDependency == nil {
			jobDependencies = append(jobDependencies, dependency.Name)
			continue
		}
		httpDependencyProto := dependency.HttpDependency
		httpDependency := dto.NewHttpDependency(httpDependencyProto.Name, httpDependencyProto.Url, httpDependencyProto.Headers, httpDependencyProto.Params)
		httpDependencies = append(httpDependencies, httpDependency)
	}
	dependencies := dto.NewDependencies(jobDependencies, httpDependencies)
	return dependencies
}

func toMetadata(jobMetadata *pb.JobMetadata) *dto.Metadata {
	metadataResourceProto := jobMetadata.Resource
	metadataResourceRequest := dto.NewResourceConfig(metadataResourceProto.Request.Cpu, metadataResourceProto.Request.Memory)
	metadataResourceLimit := dto.NewResourceConfig(metadataResourceProto.Limit.Cpu, metadataResourceProto.Limit.Memory)

	resourceMetadata := dto.NewResourceMetadata(metadataResourceRequest, metadataResourceLimit)

	metadataSchedulerProto := jobMetadata.Airflow
	schedulerMetadata := make(map[string]string)
	schedulerMetadata["pool"] = metadataSchedulerProto.Pool
	schedulerMetadata["queue"] = metadataSchedulerProto.Queue
	metadata := dto.NewMetadata(resourceMetadata, schedulerMetadata)
	return metadata
}

func toConfig(configs []*pb.JobConfigItem) *dto.Config {
	configMap := make(map[string]string, len(configs))
	for _, config := range configs {
		configMap[config.Name] = config.Value
	}
	return dto.NewConfig(configMap)
}
