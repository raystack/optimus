package v1beta1

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func fromJobProto(jobTenant tenant.Tenant, js *pb.JobSpecification) (*job.Spec, error) {
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

	upstreams := toSpecUpstreams(js.Dependencies)

	metadata := toMetadata(js.Metadata)

	return job.NewSpecBuilder(jobTenant, int(js.Version), job.Name(js.Name), js.Owner, js.Labels, schedule, window, task).
		WithDescription(js.Description).
		WithHooks(hooks).
		WithAlerts(alerts).
		WithSpecUpstream(upstreams).
		WithAssets(js.Assets).
		WithMetadata(metadata).
		Build(), nil
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

func toSpecUpstreams(upstreamProtos []*pb.JobDependency) *job.SpecUpstream {
	var upstreamNames []string
	var httpUpstreams []*job.HTTPUpstreams
	for _, upstream := range upstreamProtos {
		if upstream.HttpDependency == nil {
			upstreamNames = append(upstreamNames, upstream.Name)
			continue
		}
		httpUpstreamProto := upstream.HttpDependency
		httpUpstream := job.NewHTTPUpstream(httpUpstreamProto.Name, httpUpstreamProto.Url, httpUpstreamProto.Headers, httpUpstreamProto.Params)
		httpUpstreams = append(httpUpstreams, httpUpstream)
	}
	upstreams := job.NewSpecUpstream(upstreamNames, httpUpstreams)
	return upstreams
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
