package v1

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/models"
)

// Note: all config keys will be converted to upper case automatically
type Adapter struct {
	supportedTaskRepo      models.TransformationRepo
	supportedHookRepo      models.HookRepo
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

	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	for _, dep := range spec.Dependencies {
		dependencies[dep.GetName()] = models.JobSpecDependency{
			Type: models.JobSpecDependencyType(dep.GetType()),
		}
	}

	window, err := prepareWindow(spec.WindowSize, spec.WindowOffset, spec.WindowTruncateTo)
	if err != nil {
		return models.JobSpec{}, err
	}

	execUnit, err := adapt.supportedTaskRepo.GetByName(spec.TaskName)
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
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConfigs,
			Window: window,
		},
		Dependencies: dependencies,
		Hooks:        hooks,
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
			return window, errors.Wrapf(err, "failed to parse task window with size %v", windowSize)
		}
	}
	if windowOffset != "" {
		window.Offset, err = time.ParseDuration(windowOffset)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window with offset %v", windowOffset)
		}
	}
	return window, nil
}

func (adapt *Adapter) ToJobProto(spec models.JobSpec) (*pb.JobSpecification, error) {
	if spec.Task.Unit == nil {
		return nil, errors.New("task unit cannot be nil")
	}

	conf := &pb.JobSpecification{
		Version:          int32(spec.Version),
		Name:             spec.Name,
		Owner:            spec.Owner,
		Interval:         spec.Schedule.Interval,
		StartDate:        spec.Schedule.StartDate.Format(models.JobDatetimeLayout),
		DependsOnPast:    spec.Behavior.DependsOnPast,
		CatchUp:          spec.Behavior.CatchUp,
		TaskName:         spec.Task.Unit.Name(),
		WindowSize:       spec.Task.Window.SizeString(),
		WindowOffset:     spec.Task.Window.OffsetString(),
		WindowTruncateTo: spec.Task.Window.TruncateTo,
		Assets:           spec.Assets.ToMap(),
		Dependencies:     []*pb.JobDependency{},
		Hooks:            adapt.ToHookProto(spec.Hooks),
		Description:      spec.Description,
		Labels:           spec.Labels,
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

	taskConfigs := []*pb.JobConfigItem{}
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
	data := []*pb.InstanceSpecData{}
	for _, asset := range spec.Data {
		data = append(data, &pb.InstanceSpecData{
			Name:  asset.Name,
			Value: asset.Value,
			Type:  asset.Type,
		})
	}
	schdAt, err := ptypes.TimestampProto(spec.ScheduledAt)
	if err != nil {
		return nil, err
	}
	return &pb.InstanceSpec{
		JobName:     spec.Job.Name,
		ScheduledAt: schdAt,
		Data:        data,
		State:       spec.State,
	}, nil
}

func (adapt *Adapter) FromInstanceProto(conf *pb.InstanceSpec) (models.InstanceSpec, error) {
	data := []models.InstanceSpecData{}
	for _, asset := range conf.GetData() {
		data = append(data, models.InstanceSpecData{
			Name:  asset.Name,
			Value: asset.Value,
			Type:  asset.Type,
		})
	}
	schdAt, err := ptypes.Timestamp(conf.ScheduledAt)
	if err != nil {
		return models.InstanceSpec{}, err
	}
	return models.InstanceSpec{
		Job: models.JobSpec{
			Name: conf.JobName,
		},
		ScheduledAt: schdAt,
		Data:        data,
		State:       conf.State,
	}, nil
}

func (adapt *Adapter) FromHookProto(hooksProto []*pb.JobSpecHook) ([]models.JobSpecHook, error) {
	var hooks []models.JobSpecHook
	for _, hook := range hooksProto {
		hookUnit, err := adapt.supportedHookRepo.GetByName(hook.Name)
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

func (adapt *Adapter) ToHookProto(hooks []models.JobSpecHook) (protoHooks []*pb.JobSpecHook) {
	for _, hook := range hooks {
		hookConfigs := []*pb.JobConfigItem{}
		for _, c := range hook.Config {
			hookConfigs = append(hookConfigs, &pb.JobConfigItem{
				Name:  c.Name,
				Value: c.Value,
			})
		}

		protoHooks = append(protoHooks, &pb.JobSpecHook{
			Name:   hook.Unit.Name(),
			Config: hookConfigs,
		})
	}
	return
}

func (adapt *Adapter) ToResourceProto(spec models.ResourceSpec) (*pb.ResourceSpecification, error) {
	typeController, ok := spec.Datastore.Types()[spec.Type]
	if !ok {
		return nil, errors.New(fmt.Sprintf("unsupported type %s for datastore %s", spec.Type, spec.Datastore.Name()))
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
		return models.ResourceSpec{}, errors.New(fmt.Sprintf("unsupported type %s for datastore %s", spec.Type, storeName))
	}
	buf, err := proto.Marshal(spec)
	if err != nil {
		return models.ResourceSpec{}, err
	}
	return typeController.Adapter().FromProtobuf(buf)
}

func NewAdapter(supportedTaskRepo models.TransformationRepo,
	supportedHookRepo models.HookRepo, datastoreRepo models.DatastoreRepo) *Adapter {
	return &Adapter{
		supportedTaskRepo:      supportedTaskRepo,
		supportedHookRepo:      supportedHookRepo,
		supportedDatastoreRepo: datastoreRepo,
	}
}
