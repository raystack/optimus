package v1

import (
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/models"
)

type Adapter struct{}

func (adapt *Adapter) FromJobProto(spec *pb.JobSpecification) models.JobSpec {
	return models.JobSpec{
		Version: int(spec.Version),
		Name:    spec.Name,
		Owner:   spec.Owner,
		Schedule: models.JobSpecSchedule{
			Interval: spec.Interval,
		},
		Assets: models.JobAssets{}.FromMap(spec.Assets),
	}
	//TODO:
}

func (adapt *Adapter) ToJobProto(spec models.JobSpec) *pb.JobSpecification {
	conf := &pb.JobSpecification{
		Version:          int32(spec.Version),
		Name:             spec.Name,
		Owner:            spec.Owner,
		Interval:         spec.Schedule.Interval,
		StartDate:        spec.Schedule.StartDate.Format(models.JobDatetimeLayout),
		DependsOnPast:    spec.Behavior.DependsOnPast,
		CatchUp:          spec.Behavior.CatchUp,
		TaskName:         spec.Task.Name,
		Config:           spec.Task.Config,
		WindowSize:       spec.Task.Window.SizeString(),
		WindowOffset:     spec.Task.Window.OffsetString(),
		WindowTruncateTo: spec.Task.Window.TruncateTo,
		Assets:           spec.Assets.ToMap(),
		Dependencies:     []string{},
	}
	if spec.Schedule.EndDate != nil {
		conf.EndDate = spec.Schedule.EndDate.Format(models.JobDatetimeLayout)
	}
	for name := range spec.Dependencies {
		conf.Dependencies = append(conf.Dependencies, name)
	}

	return conf
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

func NewAdapter() *Adapter {
	return &Adapter{}
}
