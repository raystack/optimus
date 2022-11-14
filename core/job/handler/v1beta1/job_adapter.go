package v1beta1

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func fromJobProto(js *pb.JobSpecification) (*job.Spec, error) {
	var retry *job.Retry
	var alerts []*job.Alert
	if js.Behavior != nil {
		retry = toRetry(js.Behavior.Retry)
		a, err := toAlerts(js.Behavior.Notify)
		if err != nil {
			return nil, err
		}
		alerts = a
	}

	startDate, err := job.ScheduleDateFrom(js.StartDate)
	if err != nil {
		return nil, err
	}
	endDate, err := job.ScheduleDateFrom(js.EndDate)
	if err != nil {
		return nil, err
	}
	schedule, err := job.NewScheduleBuilder(startDate).
		WithInterval(js.Interval).
		WithEndDate(endDate).
		WithDependsOnPast(js.DependsOnPast).
		WithCatchUp(js.CatchUp).
		WithRetry(retry).
		Build()
	if err != nil {
		return nil, err
	}

	window, err := models.NewWindow(int(js.Version), js.WindowTruncateTo, js.WindowOffset, js.WindowSize)
	if err != nil {
		return nil, err
	}
	if err := window.Validate(); err != nil {
		return nil, err
	}

	taskConfig, err := toConfig(js.Config)
	if err != nil {
		return nil, err
	}
	taskName, err := job.TaskNameFrom(js.TaskName)
	if err != nil {
		return nil, err
	}
	task := job.NewTask(taskName, taskConfig)

	hooks, err := toHooks(js.Hooks)
	if err != nil {
		return nil, err
	}

	upstream, err := toSpecUpstreams(js.Dependencies)
	if err != nil {
		return nil, err
	}

	metadata := toMetadata(js.Metadata)

	version, err := job.VersionFrom(int(js.Version))
	if err != nil {
		return nil, err
	}
	name, err := job.NameFrom(js.Name)
	if err != nil {
		return nil, err
	}
	owner, err := job.OwnerFrom(js.Owner)
	if err != nil {
		return nil, err
	}
	asset := job.NewAsset(js.Assets)
	return job.NewSpecBuilder(version, name, owner, schedule, window, task).
		WithDescription(js.Description).
		WithLabels(js.Labels).
		WithHooks(hooks).
		WithAlerts(alerts).
		WithSpecUpstream(upstream).
		WithAsset(asset).
		WithMetadata(metadata).
		Build(), nil
}

func toRetry(protoRetry *pb.JobSpecification_Behavior_Retry) *job.Retry {
	if protoRetry == nil {
		return nil
	}
	return job.NewRetry(int(protoRetry.Count), protoRetry.Delay.GetNanos(), protoRetry.ExponentialBackoff)
}

func toHooks(hooksProto []*pb.JobSpecHook) ([]*job.Hook, error) {
	hooks := make([]*job.Hook, len(hooksProto))
	for i, hookProto := range hooksProto {
		hookConfig, err := toConfig(hookProto.Config)
		if err != nil {
			return nil, err
		}
		hookName, err := job.HookNameFrom(hookProto.Name)
		if err != nil {
			return nil, err
		}
		hooks[i] = job.NewHook(hookName, hookConfig)
	}
	return hooks, nil
}

func toAlerts(notifiers []*pb.JobSpecification_Behavior_Notifiers) ([]*job.Alert, error) {
	alerts := make([]*job.Alert, len(notifiers))
	for i, notify := range notifiers {
		alertOn := job.EventType(utils.FromEnumProto(notify.On.String(), "type"))
		config, err := job.NewConfig(notify.Config)
		if err != nil {
			return nil, err
		}
		alerts[i] = job.NewAlertBuilder(alertOn, notify.Channels).WithConfig(config).Build()
	}
	return alerts, nil
}

func toSpecUpstreams(upstreamProtos []*pb.JobDependency) (*job.SpecUpstream, error) {
	var upstreamNames []job.SpecUpstreamName
	var httpUpstreams []*job.SpecHTTPUpstream
	for _, upstream := range upstreamProtos {
		upstreamName := job.SpecUpstreamNameFrom(upstream.Name)
		if upstream.HttpDependency == nil {
			upstreamNames = append(upstreamNames, upstreamName)
			continue
		}
		httpUpstreamProto := upstream.HttpDependency
		httpUpstreamName, err := job.NameFrom(httpUpstreamProto.Name)
		if err != nil {
			return nil, err
		}
		httpUpstream := job.NewSpecHTTPUpstreamBuilder(httpUpstreamName, httpUpstreamProto.Url).
			WithHeaders(httpUpstreamProto.Headers).
			WithParams(httpUpstreamProto.Params).
			Build()
		httpUpstreams = append(httpUpstreams, httpUpstream)
	}
	upstream := job.NewSpecUpstreamBuilder().WithUpstreamNames(upstreamNames).WithSpecHTTPUpstream(httpUpstreams).Build()
	return upstream, nil
}

func toMetadata(jobMetadata *pb.JobMetadata) *job.Metadata {
	if jobMetadata == nil {
		return nil
	}

	var resourceMetadata *job.MetadataResource
	if jobMetadata.Resource != nil {
		metadataResourceProto := jobMetadata.Resource
		request := job.NewMetadataResourceConfig(metadataResourceProto.Request.Cpu, metadataResourceProto.Request.Memory)
		limit := job.NewMetadataResourceConfig(metadataResourceProto.Limit.Cpu, metadataResourceProto.Limit.Memory)
		resourceMetadata = job.NewResourceMetadata(request, limit)
	}

	schedulerMetadata := make(map[string]string)
	if jobMetadata.Airflow != nil {
		metadataSchedulerProto := jobMetadata.Airflow
		schedulerMetadata["pool"] = metadataSchedulerProto.Pool
		schedulerMetadata["queue"] = metadataSchedulerProto.Queue
	}
	return job.NewMetadataBuilder().WithResource(resourceMetadata).WithScheduler(schedulerMetadata).Build()
}

func toConfig(configs []*pb.JobConfigItem) (*job.Config, error) {
	configMap := make(map[string]string, len(configs))
	for _, config := range configs {
		configMap[config.Name] = config.Value
	}
	return job.NewConfig(configMap)
}
