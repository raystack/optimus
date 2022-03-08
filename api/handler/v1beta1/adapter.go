package v1beta1

import (
	"fmt"
	"strings"
	"time"

	"github.com/odpf/optimus/utils"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Note: all config keys will be converted to upper case automatically
type Adapter struct {
	pluginRepo             models.PluginRepository
	supportedDatastoreRepo models.DatastoreRepo
}

func (adapt *Adapter) FromJobProto(spec *pb.JobSpecification) (models.JobSpec, error) {
	startDate, err := time.Parse(models.JobDatetimeLayout, spec.StartDate)
	if err != nil {
		return models.JobSpec{}, err
	}

	var endDate *time.Time = nil
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

	execUnit, err := adapt.pluginRepo.GetByName(spec.TaskName)
	if err != nil {
		return models.JobSpec{}, err
	}

	// adapt hooks
	hooks, err := adapt.FromHookProto(spec.Hooks)
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
		metadata.Resource = adapt.FromJobSpecMetadataResourceProto(spec.Metadata.Resource)
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
	window.Size = time.Hour * 24
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

func (adapt *Adapter) ToJobProto(spec models.JobSpec) (*pb.JobSpecification, error) {
	adaptedHook, err := adapt.ToHookProto(spec.Hooks)
	if err != nil {
		return nil, err
	}

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
			Resource: adapt.ToJobSpecMetadataResourceProto(spec.Metadata.Resource),
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

	//prep external dependencies for proto
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

	return conf, nil
}

func (adapt *Adapter) ToProjectProto(spec models.ProjectSpec) *pb.ProjectSpecification {
	return &pb.ProjectSpecification{
		Name:   spec.Name,
		Config: spec.Config,
	}
}

func (adapt *Adapter) FromProjectProto(conf *pb.ProjectSpecification) models.ProjectSpec {
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
func (adapt *Adapter) ToProjectProtoWithSecrets(spec models.ProjectSpec) *pb.ProjectSpecification {
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

func (adapt *Adapter) FromProjectProtoWithSecrets(conf *pb.ProjectSpecification) models.ProjectSpec {
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

func (adapt *Adapter) ToProjectProtoWithSecret(spec models.ProjectSpec, pluginType models.InstanceType, pluginName string) *pb.ProjectSpecification {
	pluginSecretName := models.PluginSecretString(pluginType, pluginName)
	secrets := []*pb.ProjectSpecification_ProjectSecret{}
	for _, s := range spec.Secret {
		if strings.ToUpper(s.Name) != pluginSecretName {
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

func (adapt *Adapter) ToNamespaceProto(spec models.NamespaceSpec) *pb.NamespaceSpecification {
	return &pb.NamespaceSpecification{
		Name:   spec.Name,
		Config: spec.Config,
	}
}

func (adapt *Adapter) FromNamespaceProto(conf *pb.NamespaceSpecification) models.NamespaceSpec {
	namespaceConf := map[string]string{}
	for key, val := range conf.GetConfig() {
		namespaceConf[strings.ToUpper(key)] = val
	}

	return models.NamespaceSpec{
		Name:   conf.GetName(),
		Config: namespaceConf,
	}
}

func (adapt *Adapter) ToInstanceProto(spec models.InstanceSpec) (*pb.InstanceSpec, error) {
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
	}, nil
}

func (adapt *Adapter) FromInstanceProto(conf *pb.InstanceSpec) (models.InstanceSpec, error) {
	if conf == nil {
		return models.InstanceSpec{}, nil
	}
	var data []models.InstanceSpecData
	for _, asset := range conf.GetData() {
		assetType := models.InstanceDataTypeEnv
		switch asset.Type {
		case pb.InstanceSpecData_TYPE_FILE:
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

func (adapt *Adapter) FromHookProto(hooksProto []*pb.JobSpecHook) ([]models.JobSpecHook, error) {
	var hooks []models.JobSpecHook
	for _, hook := range hooksProto {
		hookUnit, err := adapt.pluginRepo.GetByName(hook.Name)
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

func (adapt *Adapter) ToHookProto(hooks []models.JobSpecHook) (protoHooks []*pb.JobSpecHook, err error) {
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

func (adapt *Adapter) ToResourceProto(spec models.ResourceSpec) (*pb.ResourceSpecification, error) {
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

func (adapt *Adapter) FromResourceProto(spec *pb.ResourceSpecification, storeName string) (models.ResourceSpec, error) {
	storer, err := adapt.supportedDatastoreRepo.GetByName(storeName)
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

func (adapt *Adapter) ToReplayExecutionTreeNode(res *tree.TreeNode) (*pb.ReplayExecutionTreeNode, error) {
	response := &pb.ReplayExecutionTreeNode{
		JobName: res.GetName(),
	}
	for _, run := range res.Runs.Values() {
		runTime := run.(time.Time)
		timestampPb := timestamppb.New(runTime)
		response.Runs = append(response.Runs, timestampPb)
	}
	for _, dep := range res.Dependents {
		parsedDep, err := adapt.ToReplayExecutionTreeNode(dep)
		if err != nil {
			return nil, err
		}
		response.Dependents = append(response.Dependents, parsedDep)
	}
	return response, nil
}

func (adapt *Adapter) ToReplayStatusTreeNode(res *tree.TreeNode) (*pb.ReplayStatusTreeNode, error) {
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
		parsedDep, err := adapt.ToReplayStatusTreeNode(dep)
		if err != nil {
			return nil, err
		}
		response.Dependents = append(response.Dependents, parsedDep)
	}
	return response, nil
}

func (adapt *Adapter) ToJobSpecMetadataResourceProto(resource models.JobSpecResource) *pb.JobSpecMetadataResource {
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

func (adapt *Adapter) FromJobSpecMetadataResourceProto(resource *pb.JobSpecMetadataResource) models.JobSpecResource {
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

func NewAdapter(pluginRepo models.PluginRepository, datastoreRepo models.DatastoreRepo) *Adapter {
	return &Adapter{
		pluginRepo:             pluginRepo,
		supportedDatastoreRepo: datastoreRepo,
	}
}
