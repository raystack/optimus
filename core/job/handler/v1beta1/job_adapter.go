package v1beta1

import (
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func toJobProto(jobEntity *job.Job) *pb.JobSpecification {
	return &pb.JobSpecification{
		Version:          int32(jobEntity.Spec().Version().Int()),
		Name:             jobEntity.Spec().Name().String(),
		Owner:            jobEntity.Spec().Owner().String(),
		StartDate:        jobEntity.Spec().Schedule().StartDate().String(),
		EndDate:          jobEntity.Spec().Schedule().EndDate().String(),
		Interval:         jobEntity.Spec().Schedule().Interval(),
		DependsOnPast:    jobEntity.Spec().Schedule().DependsOnPast(),
		CatchUp:          jobEntity.Spec().Schedule().CatchUp(),
		TaskName:         jobEntity.Spec().Task().Name().String(),
		Config:           fromConfig(jobEntity.Spec().Task().Config()),
		WindowSize:       jobEntity.Spec().Window().GetSize(),
		WindowOffset:     jobEntity.Spec().Window().GetOffset(),
		WindowTruncateTo: jobEntity.Spec().Window().GetTruncateTo(),
		Dependencies:     fromSpecUpstreams(jobEntity.Spec().Upstream()),
		Assets:           fromAsset(jobEntity.Spec().Asset()),
		Hooks:            fromHooks(jobEntity.Spec().Hooks()),
		Description:      jobEntity.Spec().Description(),
		Labels:           jobEntity.Spec().Labels(),
		Behavior:         fromRetryAndAlerts(jobEntity.Spec().Schedule().Retry(), jobEntity.Spec().Alerts()),
		Metadata:         fromMetadata(jobEntity.Spec().Metadata()),
		Destination:      jobEntity.Destination().String(),
		Sources:          fromResourceURNs(jobEntity.Sources()),
	}
}

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
	task := job.NewTaskBuilder(taskName, taskConfig).Build()

	hooks, err := toHooks(js.Hooks)
	if err != nil {
		return nil, err
	}

	upstream, err := toSpecUpstreams(js.Dependencies)
	if err != nil {
		return nil, err
	}

	metadata, err := toMetadata(js.Metadata)
	if err != nil {
		return nil, err
	}

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

	labels, err := job.NewLabels(js.Labels)
	if err != nil {
		return nil, err
	}

	asset, err := job.NewAsset(js.Assets)
	if err != nil {
		return nil, err
	}

	return job.NewSpecBuilder(version, name, owner, schedule, window, task).
		WithDescription(js.Description).
		WithLabels(labels).
		WithHooks(hooks).
		WithAlerts(alerts).
		WithSpecUpstream(upstream).
		WithAsset(asset).
		WithMetadata(metadata).
		Build(), nil
}

func fromResourceURNs(resourceURNs []job.ResourceURN) []string {
	var resources []string
	for _, resourceURN := range resourceURNs {
		resources = append(resources, resourceURN.String())
	}
	return resources
}

func fromRetryAndAlerts(jobRetry *job.Retry, alerts []*job.Alert) *pb.JobSpecification_Behavior {
	retryProto := fromRetry(jobRetry)
	notifierProto := fromAlerts(alerts)
	if retryProto == nil && len(notifierProto) == 0 {
		return nil
	}
	return &pb.JobSpecification_Behavior{
		Retry:  retryProto,
		Notify: notifierProto,
	}
}

func toRetry(protoRetry *pb.JobSpecification_Behavior_Retry) *job.Retry {
	if protoRetry == nil {
		return nil
	}
	return job.NewRetry(int(protoRetry.Count), protoRetry.Delay.GetNanos(), protoRetry.ExponentialBackoff)
}

func fromRetry(jobRetry *job.Retry) *pb.JobSpecification_Behavior_Retry {
	if jobRetry == nil {
		return nil
	}
	return &pb.JobSpecification_Behavior_Retry{
		Count:              int32(jobRetry.Count()),
		Delay:              &durationpb.Duration{Nanos: jobRetry.Delay()},
		ExponentialBackoff: jobRetry.ExponentialBackoff(),
	}
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

func fromHooks(hooks []*job.Hook) []*pb.JobSpecHook {
	var hooksProto []*pb.JobSpecHook
	for _, hook := range hooks {
		hooksProto = append(hooksProto, &pb.JobSpecHook{
			Name:   hook.Name().String(),
			Config: fromConfig(hook.Config()),
		})
	}
	return hooksProto
}

func fromAsset(jobAsset *job.Asset) map[string]string {
	var assets map[string]string
	if jobAsset != nil {
		assets = jobAsset.Assets()
	}
	return assets
}

func toAlerts(notifiers []*pb.JobSpecification_Behavior_Notifiers) ([]*job.Alert, error) {
	alerts := make([]*job.Alert, len(notifiers))
	for i, notify := range notifiers {
		alertOn := job.EventType(utils.FromEnumProto(notify.On.String(), "type"))
		config, err := job.NewConfig(notify.Config)
		if err != nil {
			return nil, err
		}
		alertConfig := job.NewAlertBuilder(alertOn, notify.Channels).WithConfig(config).Build()
		if err = alertConfig.Validate(); err != nil {
			return nil, err
		}
		alerts[i] = alertConfig
	}
	return alerts, nil
}

func fromAlerts(jobAlerts []*job.Alert) []*pb.JobSpecification_Behavior_Notifiers {
	var notifiers []*pb.JobSpecification_Behavior_Notifiers
	for _, alert := range jobAlerts {
		notifiers = append(notifiers, &pb.JobSpecification_Behavior_Notifiers{
			On:       pb.JobEvent_Type(pb.JobEvent_Type_value[utils.ToEnumProto(string(alert.On()), "type")]),
			Channels: alert.Channels(),
			Config:   alert.Config().Configs(),
		})
	}
	return notifiers
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
	if err := upstream.Validate(); err != nil {
		return nil, err
	}
	return upstream, nil
}

func fromSpecUpstreams(upstreams *job.SpecUpstream) []*pb.JobDependency {
	if upstreams == nil {
		return nil
	}
	var dependencies []*pb.JobDependency
	for _, upstreamName := range upstreams.UpstreamNames() {
		dependencies = append(dependencies, &pb.JobDependency{Name: upstreamName.String()}) // TODO: upstream type?
	}
	for _, httpUpstream := range upstreams.HTTPUpstreams() {
		dependencies = append(dependencies, &pb.JobDependency{
			HttpDependency: &pb.HttpDependency{
				Name:    httpUpstream.Name().String(),
				Url:     httpUpstream.URL(),
				Headers: httpUpstream.Headers(),
				Params:  httpUpstream.Params(),
			},
		})
	}
	return dependencies
}

func toMetadata(jobMetadata *pb.JobMetadata) (*job.Metadata, error) {
	if jobMetadata == nil {
		return nil, nil
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
	metadata := job.NewMetadataBuilder().WithResource(resourceMetadata).WithScheduler(schedulerMetadata).Build()
	if err := metadata.Validate(); err != nil {
		return nil, err
	}
	return metadata, nil
}

func fromMetadata(metadata *job.Metadata) *pb.JobMetadata {
	if metadata == nil {
		return nil
	}

	var metadataResourceProto *pb.JobSpecMetadataResource
	if metadata.Resource() != nil {
		metadataResourceProto.Request = &pb.JobSpecMetadataResourceConfig{
			Cpu:    metadata.Resource().Request().CPU(),
			Memory: metadata.Resource().Request().Memory(),
		}
		metadataResourceProto.Limit = &pb.JobSpecMetadataResourceConfig{
			Cpu:    metadata.Resource().Limit().CPU(),
			Memory: metadata.Resource().Limit().Memory(),
		}
	}

	var metadataSchedulerProto *pb.JobSpecMetadataAirflow
	if metadata.Scheduler() != nil {
		metadataSchedulerProto.Pool = metadata.Scheduler()["pool"]
		metadataSchedulerProto.Queue = metadata.Scheduler()["queue"]
	}
	return &pb.JobMetadata{
		Resource: metadataResourceProto,
		Airflow:  metadataSchedulerProto,
	}
}

func toConfig(configs []*pb.JobConfigItem) (*job.Config, error) {
	configMap := make(map[string]string, len(configs))
	for _, config := range configs {
		configMap[config.Name] = config.Value
	}
	return job.NewConfig(configMap)
}

func fromConfig(jobConfig *job.Config) []*pb.JobConfigItem {
	configs := []*pb.JobConfigItem{}
	for configName, configValue := range jobConfig.Configs() {
		configs = append(configs, &pb.JobConfigItem{Name: configName, Value: configValue})
	}
	return configs
}

func toBasicInfoSectionProto(jobDetail *job.Job, logMessages []*pb.Log) *pb.JobInspectResponse_BasicInfoSection {
	var sources []string
	for _, source := range jobDetail.Sources() {
		sources = append(sources, source.String())
	}
	return &pb.JobInspectResponse_BasicInfoSection{
		Destination: jobDetail.Destination().String(),
		Source:      sources,
		Job:         toJobProto(jobDetail),
		Notice:      logMessages,
	}
}

func toUpstreamProtos(upstreams []*job.Upstream) ([]*pb.JobInspectResponse_JobDependency, []*pb.JobInspectResponse_JobDependency, []*pb.JobInspectResponse_UpstreamSection_UnknownDependencies) {
	var internalUpstreamProtos []*pb.JobInspectResponse_JobDependency
	var externalUpstreamProtos []*pb.JobInspectResponse_JobDependency
	var unknownUpstreamProtos []*pb.JobInspectResponse_UpstreamSection_UnknownDependencies
	for _, upstream := range upstreams {
		if upstream.State() != job.UpstreamStateResolved {
			if upstream.Type() == job.UpstreamTypeStatic {
				unknownUpstreamProtos = append(unknownUpstreamProtos, &pb.JobInspectResponse_UpstreamSection_UnknownDependencies{
					JobName:     upstream.Name().String(),
					ProjectName: upstream.ProjectName().String(),
				})
			}
			continue
		}
		upstreamProto := &pb.JobInspectResponse_JobDependency{
			Name:          upstream.Name().String(),
			Host:          upstream.Host(),
			ProjectName:   upstream.ProjectName().String(),
			NamespaceName: upstream.NamespaceName().String(),
			TaskName:      upstream.TaskName().String(),
		}
		if upstream.External() {
			externalUpstreamProtos = append(externalUpstreamProtos, upstreamProto)
		} else {
			internalUpstreamProtos = append(internalUpstreamProtos, upstreamProto)
		}
	}
	return internalUpstreamProtos, externalUpstreamProtos, unknownUpstreamProtos
}

func toHTTPUpstreamProtos(httpUpstreamSpecs []*job.SpecHTTPUpstream) []*pb.HttpDependency {
	var httpUpstreamProtos []*pb.HttpDependency
	for _, httpUpstream := range httpUpstreamSpecs {
		httpUpstreamProtos = append(httpUpstreamProtos, &pb.HttpDependency{
			Name:    httpUpstream.Name().String(),
			Url:     httpUpstream.URL(),
			Headers: httpUpstream.Headers(),
			Params:  httpUpstream.Params(),
		})
	}
	return httpUpstreamProtos
}

func toDownstreamProtos(downstreamJobs []*dto.Downstream) []*pb.JobInspectResponse_JobDependency {
	var downstreamProtos []*pb.JobInspectResponse_JobDependency
	for _, downstreamJob := range downstreamJobs {
		downstreamProtos = append(downstreamProtos, &pb.JobInspectResponse_JobDependency{
			Name:          downstreamJob.Name,
			ProjectName:   downstreamJob.ProjectName,
			NamespaceName: downstreamJob.NamespaceName,
			TaskName:      downstreamJob.TaskName,
		})
	}
	return downstreamProtos
}
