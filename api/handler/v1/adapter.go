package v1

import (
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/models"
)

type Adapter struct {
	supportedTaskRepo models.SupportedTaskRepo
	supportedHookRepo models.SupportedHookRepo
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
		dependencies[dep] = models.JobSpecDependency{}
	}

	window, err := prepareWindow(spec)
	if err != nil {
		return models.JobSpec{}, err
	}

	execUnit, err := adapt.supportedTaskRepo.GetByName(spec.TaskName)
	if err != nil {
		return models.JobSpec{}, err
	}

	// adapt hooks
	hooks, err := adapt.fromHookProto(spec.Hooks)
	if err != nil {
		return models.JobSpec{}, err
	}

	return models.JobSpec{
		Version: int(spec.Version),
		Name:    spec.Name,
		Owner:   spec.Owner,
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
			Config: spec.Config,
			Window: window,
		},
		Dependencies: dependencies,
		Hooks:        hooks,
	}, nil
}

func prepareWindow(spec *pb.JobSpecification) (models.JobSpecTaskWindow, error) {
	var err error
	window := models.JobSpecTaskWindow{}
	window.Size = time.Hour * 24
	window.Offset = 0
	window.TruncateTo = "d"

	if spec.WindowTruncateTo != "" {
		window.TruncateTo = spec.WindowTruncateTo
	}
	if spec.WindowSize != "" {
		window.Size, err = time.ParseDuration(spec.WindowSize)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window of %v with size %v", spec.Name, spec.WindowSize)
		}
	}
	if spec.WindowOffset != "" {
		window.Offset, err = time.ParseDuration(spec.WindowOffset)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window of %v with offset %v", spec.Name, spec.WindowOffset)
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
		TaskName:         spec.Task.Unit.GetName(),
		Config:           spec.Task.Config,
		WindowSize:       spec.Task.Window.SizeString(),
		WindowOffset:     spec.Task.Window.OffsetString(),
		WindowTruncateTo: spec.Task.Window.TruncateTo,
		Assets:           spec.Assets.ToMap(),
		Dependencies:     []string{},
		Hooks:            adapt.toHookProto(spec.Hooks),
	}
	if spec.Schedule.EndDate != nil {
		conf.EndDate = spec.Schedule.EndDate.Format(models.JobDatetimeLayout)
	}
	for name := range spec.Dependencies {
		conf.Dependencies = append(conf.Dependencies, name)
	}

	return conf, nil
}

func (adapt *Adapter) ToProjectProto(spec models.ProjectSpec) *pb.ProjectSpecification {
	return &pb.ProjectSpecification{
		Name:   spec.Name,
		Config: spec.Config,
	}
}

func (adapt *Adapter) FromProjectProto(conf *pb.ProjectSpecification) models.ProjectSpec {
	return models.ProjectSpec{
		Name:   conf.GetName(),
		Config: conf.GetConfig(),
	}
}

func (adapt *Adapter) ToInstanceProto(spec models.InstanceSpec) (*pb.InstanceSpec, error) {
	data := []*pb.InstanceSpecData{}
	for _, asset := range spec.Data {
		data = append(data, &pb.InstanceSpecData{
			Name:  asset.Name,
			Value: asset.Value,
			Task:  asset.Task,
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
			Task:  asset.Task,
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

func (adapt *Adapter) fromHookProto(hooksProto []*pb.JobSpecHook) ([]models.JobSpecHook, error) {
	var hooks []models.JobSpecHook
	for _, hook := range hooksProto {
		hookUnit, err := adapt.supportedHookRepo.GetByName(hook.Name)
		if err != nil {
			return nil, err
		}
		hooks = append(hooks, models.JobSpecHook{
			Type:   hook.Type,
			Config: hook.Config,
			Unit:   hookUnit,
		})
	}
	return hooks, nil
}

func (adapt *Adapter) toHookProto(hooks []models.JobSpecHook) (protoHooks []*pb.JobSpecHook) {
	for _, hook := range hooks {
		protoHooks = append(protoHooks, &pb.JobSpecHook{
			Name:   hook.Unit.GetName(),
			Type:   hook.Type,
			Config: hook.Config,
		})
	}
	return
}

func NewAdapter(supportedTaskRepo models.SupportedTaskRepo, supportedHookRepo models.SupportedHookRepo) *Adapter {
	return &Adapter{
		supportedTaskRepo: supportedTaskRepo,
		supportedHookRepo: supportedHookRepo,
	}
}
