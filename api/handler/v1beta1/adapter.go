package v1beta1

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

const HoursInDay = time.Hour * 24

func FromJobProto(spec *pb.JobSpecification, pluginRepo models.PluginRepository) (models.JobSpec, error) {
	startDate, err := time.Parse(models.JobDatetimeLayout, spec.StartDate)
	if err != nil {
		return models.JobSpec{}, err
	}

	var endDate *time.Time
	if spec.EndDate != "" {
		end, err := time.Parse(models.JobDatetimeLayout, spec.EndDate)
		if err != nil {
			return models.JobSpec{}, err
		}
		endDate = &end
	}
	// prep external dependencies
	var externalDependencies models.ExternalDependency
	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	for _, dep := range spec.Dependencies {
		if dep.GetName() != "" {
			dependencies[dep.GetName()] = models.JobSpecDependency{
				Type: models.JobSpecDependencyType(dep.GetType()),
			}
		}
		if dep.HttpDependency != nil {
			externalDependencies.HTTPDependencies = append(externalDependencies.HTTPDependencies, models.HTTPDependency{
				Name:          dep.HttpDependency.Name,
				RequestParams: dep.HttpDependency.Params,
				URL:           dep.HttpDependency.Url,
				Headers:       dep.HttpDependency.Headers,
			})
		}
	}

	window, err := prepareWindow(spec.WindowSize, spec.WindowOffset, spec.WindowTruncateTo)
	if err != nil {
		return models.JobSpec{}, err
	}

	execUnit, err := pluginRepo.GetByName(spec.TaskName)
	if err != nil {
		return models.JobSpec{}, err
	}

	// adapt hooks
	hooks, err := FromHookProto(spec.Hooks, pluginRepo)
	if err != nil {
		return models.JobSpec{}, err
	}

	taskConfigs := models.JobSpecConfigs{}
	for _, l := range spec.Config {
		taskConfigs = append(taskConfigs, models.JobSpecConfigItem{
			Name:  l.Name,
			Value: l.Value,
		})
	}

	retryDelay := time.Duration(0)
	retryCount := 0
	retryExponentialBackoff := false
	var notifiers []models.JobSpecNotifier
	if spec.Behavior != nil {
		if spec.Behavior.Retry != nil {
			retryCount = int(spec.Behavior.Retry.Count)
			retryExponentialBackoff = spec.Behavior.Retry.ExponentialBackoff
			if spec.Behavior.Retry.Delay != nil && spec.Behavior.Retry.Delay.IsValid() {
				retryDelay = spec.Behavior.Retry.Delay.AsDuration()
			}
		}

		for _, notify := range spec.Behavior.Notify {
			notifiers = append(notifiers, models.JobSpecNotifier{
				On:       models.JobEventType(utils.FromEnumProto(notify.On.String(), "type")),
				Config:   notify.Config,
				Channels: notify.Channels,
			})
		}
	}

	metadata := models.JobSpecMetadata{}
	if spec.Metadata != nil {
		metadata.Resource = FromJobSpecMetadataResourceProto(spec.Metadata.Resource)
		metadata.Airflow = FromJobSpecMetadataAirflowProto(spec.Metadata.Airflow)
	}
	return models.JobSpec{
		Version:     int(spec.Version),
		Name:        spec.Name,
		Owner:       spec.Owner,
		Description: spec.Description,
		Labels:      spec.Labels,
		Schedule: models.JobSpecSchedule{
			Interval:  spec.Interval,
			StartDate: startDate,
			EndDate:   endDate,
		},
		Assets: models.JobAssets{}.FromMap(spec.Assets),
		Behavior: models.JobSpecBehavior{
			DependsOnPast: spec.DependsOnPast,
			CatchUp:       spec.CatchUp,
			Retry: models.JobSpecBehaviorRetry{
				Count:              retryCount,
				Delay:              retryDelay,
				ExponentialBackoff: retryExponentialBackoff,
			},
			Notify: notifiers,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConfigs,
			Window: window,
		},
		Dependencies:         dependencies,
		Hooks:                hooks,
		Metadata:             metadata,
		ExternalDependencies: externalDependencies,
	}, nil
}

func prepareWindow(windowSize, windowOffset, truncateTo string) (models.JobSpecTaskWindow, error) {
	var err error
	window := models.JobSpecTaskWindow{}
	window.Size = HoursInDay
	window.Offset = 0
	window.TruncateTo = "d"

	if truncateTo != "" {
		window.TruncateTo = truncateTo
	}
	if windowSize != "" {
		window.Size, err = time.ParseDuration(windowSize)
		if err != nil {
			return window, fmt.Errorf("failed to parse task window with size %v: %w", windowSize, err)
		}
	}
	if windowOffset != "" {
		window.Offset, err = time.ParseDuration(windowOffset)
		if err != nil {
			return window, fmt.Errorf("failed to parse task window with offset %v: %w", windowOffset, err)
		}
	}
	return window, nil
}

func ToJobProto(spec models.JobSpec) *pb.JobSpecification {
	adaptedHook := ToHookProto(spec.Hooks)

	var notifyProto []*pb.JobSpecification_Behavior_Notifiers
	for _, notify := range spec.Behavior.Notify {
		notifyProto = append(notifyProto, &pb.JobSpecification_Behavior_Notifiers{
			On:       pb.JobEvent_Type(pb.JobEvent_Type_value[utils.ToEnumProto(string(notify.On), "type")]),
			Channels: notify.Channels,
			Config:   notify.Config,
		})
	}

	conf := &pb.JobSpecification{
		Version:          int32(spec.Version),
		Name:             spec.Name,
		Owner:            spec.Owner,
		Interval:         spec.Schedule.Interval,
		StartDate:        spec.Schedule.StartDate.Format(models.JobDatetimeLayout),
		DependsOnPast:    spec.Behavior.DependsOnPast,
		CatchUp:          spec.Behavior.CatchUp,
		TaskName:         spec.Task.Unit.Info().Name,
		WindowSize:       spec.Task.Window.SizeString(),
		WindowOffset:     spec.Task.Window.OffsetString(),
		WindowTruncateTo: spec.Task.Window.TruncateTo,
		Assets:           spec.Assets.ToMap(),
		Dependencies:     []*pb.JobDependency{},
		Hooks:            adaptedHook,
		Description:      spec.Description,
		Labels:           spec.Labels,
		Behavior: &pb.JobSpecification_Behavior{
			Retry: &pb.JobSpecification_Behavior_Retry{
				Count:              int32(spec.Behavior.Retry.Count),
				Delay:              durationpb.New(spec.Behavior.Retry.Delay),
				ExponentialBackoff: spec.Behavior.Retry.ExponentialBackoff,
			},
			Notify: notifyProto,
		},
		Metadata: &pb.JobMetadata{
			Resource: ToJobSpecMetadataResourceProto(spec.Metadata.Resource),
			Airflow:  ToJobSpecMetadataAirflowProto(spec.Metadata.Airflow),
		},
	}
	if spec.Schedule.EndDate != nil {
		conf.EndDate = spec.Schedule.EndDate.Format(models.JobDatetimeLayout)
	}
	for name, dep := range spec.Dependencies {
		conf.Dependencies = append(conf.Dependencies, &pb.JobDependency{
			Name: name,
			Type: dep.Type.String(),
		})
	}

	// prep external dependencies for proto
	for _, httpDep := range spec.ExternalDependencies.HTTPDependencies {
		conf.Dependencies = append(conf.Dependencies, &pb.JobDependency{
			HttpDependency: &pb.HttpDependency{
				Name:    httpDep.Name,
				Url:     httpDep.URL,
				Headers: httpDep.Headers,
				Params:  httpDep.RequestParams,
			},
		})
	}

	var taskConfigs []*pb.JobConfigItem
	for _, c := range spec.Task.Config {
		taskConfigs = append(taskConfigs, &pb.JobConfigItem{
			Name:  strings.ToUpper(c.Name),
			Value: c.Value,
		})
	}
	conf.Config = taskConfigs

	return conf
}

func ToProjectProto(spec models.ProjectSpec) *pb.ProjectSpecification {
	return &pb.ProjectSpecification{
		Name:   spec.Name,
		Config: spec.Config,
	}
}

func FromProjectProto(conf *pb.ProjectSpecification) models.ProjectSpec {
	pConf := map[string]string{}
	for key, val := range conf.GetConfig() {
		pConf[strings.ToUpper(key)] = val
	}
	return models.ProjectSpec{
		Name:   conf.GetName(),
		Config: pConf,
	}
}

// ToProjectProtoWithSecrets is unused, TODO: delete
func ToProjectProtoWithSecrets(spec models.ProjectSpec) *pb.ProjectSpecification {
	secrets := []*pb.ProjectSpecification_ProjectSecret{}
	for _, s := range spec.Secret {
		secrets = append(secrets, &pb.ProjectSpecification_ProjectSecret{
			Name:  s.Name,
			Value: s.Value,
		})
	}
	return &pb.ProjectSpecification{
		Name:    spec.Name,
		Config:  spec.Config,
		Secrets: secrets,
	}
}

func FromProjectProtoWithSecrets(conf *pb.ProjectSpecification) models.ProjectSpec {
	if conf == nil {
		return models.ProjectSpec{}
	}
	pConf := map[string]string{}
	if conf.GetConfig() != nil {
		for key, val := range conf.GetConfig() {
			pConf[strings.ToUpper(key)] = val
		}
	}
	pSec := models.ProjectSecrets{}
	if conf.GetSecrets() != nil {
		for _, s := range conf.GetSecrets() {
			pSec = append(pSec, models.ProjectSecretItem{
				Name:  s.Name,
				Value: s.Value,
			})
		}
	}
	return models.ProjectSpec{
		Name:   conf.GetName(),
		Config: pConf,
		Secret: pSec,
	}
}

func ToProjectProtoWithSecret(spec models.ProjectSpec, pluginType models.InstanceType, pluginName string) *pb.ProjectSpecification {
	pluginSecretName := models.PluginSecretString(pluginType, pluginName)
	secrets := []*pb.ProjectSpecification_ProjectSecret{}
	for _, s := range spec.Secret {
		if !strings.EqualFold(s.Name, pluginSecretName) {
			continue
		}
		secrets = append(secrets, &pb.ProjectSpecification_ProjectSecret{
			Name:  s.Name,
			Value: s.Value,
		})
	}
	return &pb.ProjectSpecification{
		Name:    spec.Name,
		Config:  spec.Config,
		Secrets: secrets,
	}
}

func ToNamespaceProto(spec models.NamespaceSpec) *pb.NamespaceSpecification {
	return &pb.NamespaceSpecification{
		Name:   spec.Name,
		Config: spec.Config,
	}
}

func FromNamespaceProto(conf *pb.NamespaceSpecification) models.NamespaceSpec {
	namespaceConf := map[string]string{}
	for key, val := range conf.GetConfig() {
		namespaceConf[strings.ToUpper(key)] = val
	}

	return models.NamespaceSpec{
		Name:   conf.GetName(),
		Config: namespaceConf,
	}
}

func ToInstanceProto(spec models.InstanceSpec) *pb.InstanceSpec {
	var data []*pb.InstanceSpecData
	for _, asset := range spec.Data {
		data = append(data, &pb.InstanceSpecData{
			Name:  asset.Name,
			Value: asset.Value,
			Type:  pb.InstanceSpecData_Type(pb.InstanceSpecData_Type_value[utils.ToEnumProto(asset.Type, "type")]),
		})
	}
	return &pb.InstanceSpec{
		State:      spec.Status.String(),
		Data:       data,
		ExecutedAt: timestamppb.New(spec.ExecutedAt),
		Name:       spec.Name,
		Type:       pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(spec.Type.String(), "type")]),
	}
}

func FromInstanceProto(conf *pb.InstanceSpec) (models.InstanceSpec, error) {
	if conf == nil {
		return models.InstanceSpec{}, nil
	}
	var data []models.InstanceSpecData
	for _, asset := range conf.GetData() {
		assetType := models.InstanceDataTypeEnv
		if asset.Type == pb.InstanceSpecData_TYPE_FILE {
			assetType = models.InstanceDataTypeFile
		}
		data = append(data, models.InstanceSpecData{
			Name:  asset.Name,
			Value: asset.Value,
			Type:  assetType,
		})
	}
	instanceType, err := models.ToInstanceType(utils.FromEnumProto(conf.Type.String(), "type"))
	if err != nil {
		return models.InstanceSpec{}, err
	}
	return models.InstanceSpec{
		Name:       conf.Name,
		Type:       instanceType,
		ExecutedAt: conf.ExecutedAt.AsTime(),
		Status:     models.JobRunState(conf.State),
		Data:       data,
	}, nil
}

func FromHookProto(hooksProto []*pb.JobSpecHook, pluginRepo models.PluginRepository) ([]models.JobSpecHook, error) {
	var hooks []models.JobSpecHook
	for _, hook := range hooksProto {
		hookUnit, err := pluginRepo.GetByName(hook.Name)
		if err != nil {
			return nil, err
		}

		configs := models.JobSpecConfigs{}
		for _, l := range hook.Config {
			configs = append(configs, models.JobSpecConfigItem{
				Name:  strings.ToUpper(l.Name),
				Value: l.Value,
			})
		}

		hooks = append(hooks, models.JobSpecHook{
			Config: configs,
			Unit:   hookUnit,
		})
	}
	return hooks, nil
}

func ToHookProto(hooks []models.JobSpecHook) (protoHooks []*pb.JobSpecHook) {
	for _, hook := range hooks {
		hookConfigs := []*pb.JobConfigItem{}
		for _, c := range hook.Config {
			hookConfigs = append(hookConfigs, &pb.JobConfigItem{
				Name:  c.Name,
				Value: c.Value,
			})
		}

		protoHooks = append(protoHooks, &pb.JobSpecHook{
			Name:   hook.Unit.Info().Name,
			Config: hookConfigs,
		})
	}
	return
}

func ToResourceProto(spec models.ResourceSpec) (*pb.ResourceSpecification, error) {
	typeController, ok := spec.Datastore.Types()[spec.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported type %s for datastore %s", spec.Type, spec.Datastore.Name())
	}
	buf, err := typeController.Adapter().ToProtobuf(spec)
	if err != nil {
		return nil, err
	}

	protoSpec := &pb.ResourceSpecification{}
	if err := proto.Unmarshal(buf, protoSpec); err != nil {
		return nil, err
	}
	return protoSpec, nil
}

func FromResourceProto(spec *pb.ResourceSpecification, storeName string, datastoreRepo models.DatastoreRepo) (models.ResourceSpec, error) {
	storer, err := datastoreRepo.GetByName(storeName)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	typeController, ok := storer.Types()[models.ResourceType(spec.GetType())]
	if !ok {
		return models.ResourceSpec{}, fmt.Errorf("unsupported type %s for datastore %s", spec.Type, storeName)
	}
	buf, err := proto.Marshal(spec)
	if err != nil {
		return models.ResourceSpec{}, err
	}
	return typeController.Adapter().FromProtobuf(buf)
}

func ToReplayExecutionTreeNode(res *tree.TreeNode) (*pb.ReplayExecutionTreeNode, error) {
	response := &pb.ReplayExecutionTreeNode{
		JobName: res.GetName(),
	}
	for _, run := range res.Runs.Values() {
		runTime := run.(time.Time)
		timestampPb := timestamppb.New(runTime)
		response.Runs = append(response.Runs, timestampPb)
	}
	for _, dep := range res.Dependents {
		parsedDep, err := ToReplayExecutionTreeNode(dep)
		if err != nil {
			return nil, err
		}
		response.Dependents = append(response.Dependents, parsedDep)
	}
	return response, nil
}

func ToReplayStatusTreeNode(res *tree.TreeNode) (*pb.ReplayStatusTreeNode, error) {
	response := &pb.ReplayStatusTreeNode{
		JobName: res.GetName(),
	}
	for _, run := range res.Runs.Values() {
		runStatus := run.(models.JobStatus)
		runStatusPb := &pb.ReplayStatusRun{
			Run:   timestamppb.New(runStatus.ScheduledAt),
			State: runStatus.State.String(),
		}
		response.Runs = append(response.Runs, runStatusPb)
	}
	for _, dep := range res.Dependents {
		parsedDep, err := ToReplayStatusTreeNode(dep)
		if err != nil {
			return nil, err
		}
		response.Dependents = append(response.Dependents, parsedDep)
	}
	return response, nil
}

func ToJobSpecMetadataResourceProto(resource models.JobSpecResource) *pb.JobSpecMetadataResource {
	if resource.Request.CPU == "" && resource.Request.Memory == "" &&
		resource.Limit.CPU == "" && resource.Limit.Memory == "" {
		return nil
	}
	output := &pb.JobSpecMetadataResource{}
	if resource.Request.CPU != "" {
		output.Request = &pb.JobSpecMetadataResourceConfig{
			Cpu: resource.Request.CPU,
		}
	}
	if resource.Request.Memory != "" {
		if output.Request == nil {
			output.Request = &pb.JobSpecMetadataResourceConfig{}
		}
		output.Request.Memory = resource.Request.Memory
	}
	if resource.Limit.CPU != "" {
		output.Limit = &pb.JobSpecMetadataResourceConfig{
			Cpu: resource.Limit.CPU,
		}
	}
	if resource.Limit.Memory != "" {
		if output.Limit == nil {
			output.Limit = &pb.JobSpecMetadataResourceConfig{}
		}
		output.Limit.Memory = resource.Limit.Memory
	}
	return output
}

func ToJobSpecMetadataAirflowProto(airflow models.JobSpecAirflow) *pb.JobSpecMetadataAirflow {
	var output *pb.JobSpecMetadataAirflow
	if airflow.Pool != "" || airflow.Queue != "" {
		output = &pb.JobSpecMetadataAirflow{
			Pool:  airflow.Pool,
			Queue: airflow.Queue,
		}
	}
	return output
}

func FromJobSpecMetadataResourceProto(resource *pb.JobSpecMetadataResource) models.JobSpecResource {
	var output models.JobSpecResource
	if resource != nil {
		if resource.Request != nil {
			output.Request.Memory = resource.Request.Memory
			output.Request.CPU = resource.Request.Cpu
		}
		if resource.Limit != nil {
			output.Limit.Memory = resource.Limit.Memory
			output.Limit.CPU = resource.Limit.Cpu
		}
	}
	return output
}

func FromJobSpecMetadataAirflowProto(airflow *pb.JobSpecMetadataAirflow) models.JobSpecAirflow {
	var output models.JobSpecAirflow
	if airflow != nil {
		output.Pool = airflow.Pool
		output.Queue = airflow.Queue
	}
	return output
}
