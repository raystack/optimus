package v1

import (
	"time"

	"github.com/pkg/errors"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/models"
)

type Adapter struct {
	supportedTaskRepo models.SupportedTaskRepo
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

func NewAdapter(supportedTaskRepo models.SupportedTaskRepo) *Adapter {
	return &Adapter{
		supportedTaskRepo: supportedTaskRepo,
	}
}
