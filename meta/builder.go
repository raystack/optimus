package meta

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "github.com/odpf/optimus/proton/src/odpf/proton/metadata/resource"
	"github.com/odpf/optimus/models"
	"time"
)

type Builder struct {
}

func (a Builder) FromJobSpec(jobSpec models.JobSpec) (*models.ResourceMetadata, error) {
	taskDestination, err := jobSpec.Task.Unit.GenerateDestination(models.UnitData{
		Config: jobSpec.Task.Config,
		Assets: jobSpec.Assets.ToMap(),
	})
	if err != nil {
		return nil, err
	}
	taskMetadata := models.TaskMetadata{
		Name:        jobSpec.Task.Unit.GetName(),
		Image:       jobSpec.Task.Unit.GetImage(),
		Description: jobSpec.Task.Unit.GetDescription(),
		Destination: taskDestination,
		Config:      jobSpec.Task.Config,
		Window:      jobSpec.Task.Window,
		Priority:    jobSpec.Task.Priority,
	}

	resourceMetadata := models.ResourceMetadata{
		Urn:          jobSpec.Name,
		Version:      jobSpec.Version,
		Description:  jobSpec.Description,
		Labels:       jobSpec.Labels,
		Owner:        jobSpec.Owner,
		Task:         taskMetadata,
		Schedule:     jobSpec.Schedule,
		Behavior:     jobSpec.Behavior,
		Dependencies: []models.JobDependencyMetadata{},
		Hooks:        []models.HookMetadata{},
	}

	for _, depJob := range jobSpec.Dependencies {
		resourceMetadata.Dependencies = append(resourceMetadata.Dependencies, models.JobDependencyMetadata{
			Project: depJob.Project.Name,
			Job:     depJob.Job.Name,
			Type:    depJob.Type.String(),
		})
	}

	for _, hook := range jobSpec.Hooks {
		resourceMetadata.Hooks = append(resourceMetadata.Hooks, models.HookMetadata{
			Name:        hook.Unit.GetName(),
			Image:       hook.Unit.GetImage(),
			Description: hook.Unit.GetDescription(),
			Config:      hook.Config,
			Type:        hook.Unit.GetType(),
			DependsOn:   hook.Unit.GetDependsOn(),
		})
	}

	return &resourceMetadata, nil
}

func (a Builder) CompileKey(urn string) ([]byte, error) {
	return proto.Marshal(&pb.OptimusLogKey{
		Urn: urn,
	})
}

func (a Builder) CompileMessage(resource *models.ResourceMetadata) ([]byte, error) {
	timestamp, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		return nil, err
	}

	jobSchedule, err := a.compileJobSchedule(resource)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(&pb.OptimusLogMessage{
		Urn:         resource.Urn,
		Version:     int32(resource.Version),
		Description: resource.Description,
		Labels:      a.compileLabels(resource),
		Owner:       resource.Owner,
		Task:        a.compileTask(resource),
		Schedule:    jobSchedule,
		Behaviour: &pb.OptimusJobBehavior{
			DependsOnPast: resource.Behavior.DependsOnPast,
			Catchup:       resource.Behavior.CatchUp,
		},
		Hooks:          a.compileHooks(resource),
		Dependencies:   a.compileDependency(resource),
		EventTimestamp: timestamp,
	})
}

func (a Builder) compileTask(resource *models.ResourceMetadata) *pb.OptimusTask {
	var taskConfig []*pb.OptimusConfig
	for _, config := range resource.Task.Config {
		taskConfig = append(taskConfig, &pb.OptimusConfig{
			Name:  config.Name,
			Value: config.Value,
		})
	}

	taskWindow := &pb.OptimusTaskWindow{
		Size:       resource.Task.Window.Size.String(),
		Offset:     resource.Task.Window.Offset.String(),
		TruncateTo: resource.Task.Window.TruncateTo,
	}

	return &pb.OptimusTask{
		Name:        resource.Task.Name,
		Image:       resource.Task.Image,
		Description: resource.Task.Description,
		Destination: resource.Task.Destination,
		Config:      taskConfig,
		Window:      taskWindow,
		Priority:    int32(resource.Task.Priority),
	}
}

func (a Builder) compileHooks(resource *models.ResourceMetadata) (hooks []*pb.OptimusHook) {
	for _, hook := range resource.Hooks {
		var hookConfig []*pb.OptimusConfig
		for _, config := range hook.Config {
			hookConfig = append(hookConfig, &pb.OptimusConfig{
				Name:  config.Name,
				Value: config.Value,
			})
		}
		hooks = append(hooks, &pb.OptimusHook{
			Name:        hook.Name,
			Image:       hook.Image,
			Description: hook.Description,
			Config:      hookConfig,
			Type:        hook.Type.String(),
			DependsOn:   hook.DependsOn,
		})
	}
	return
}

func (a Builder) compileJobSchedule(resource *models.ResourceMetadata) (*pb.OptimusJobSchedule, error) {
	scheduleStartDate, err := ptypes.TimestampProto(resource.Schedule.StartDate)
	if err != nil {
		return nil, err
	}

	var scheduleEndDate *timestamppb.Timestamp
	if resource.Schedule.EndDate != nil {
		scheduleEndDate, err = ptypes.TimestampProto(*resource.Schedule.EndDate)
		if err != nil {
			return nil, err
		}
	}

	return &pb.OptimusJobSchedule{
		StartDate: scheduleStartDate,
		EndDate:   scheduleEndDate,
		Interval:  resource.Schedule.Interval,
	}, nil
}

func (a Builder) compileDependency(resource *models.ResourceMetadata) (dependencies []*pb.OptimusJobDependency) {
	for _, dependency := range resource.Dependencies {
		dependencies = append(dependencies, &pb.OptimusJobDependency{
			Project: dependency.Project,
			Job:     dependency.Job,
			Type:    dependency.Type,
		})
	}
	return
}

func (a Builder) compileLabels(resource *models.ResourceMetadata) (labels []*pb.OptimusConfig) {
	for _, config := range resource.Labels {
		labels = append(labels, &pb.OptimusConfig{
			Name:  config.Name,
			Value: config.Value,
		})
	}
	return
}
