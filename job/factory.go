package job

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

const (
	datetimeLayout = "2006-01-02"
)

type Factory struct{}

func (fac *Factory) CreateJobSpec(inputs models.JobInput) (models.JobSpec, error) {
	var err error
	// parse dates
	startDate, err := time.Parse(datetimeLayout, inputs.Schedule.StartDate)
	if err != nil {
		return models.JobSpec{}, err
	}
	var endDate *time.Time = nil
	if inputs.Schedule.EndDate != "" {
		end, err := time.Parse(datetimeLayout, inputs.Schedule.EndDate)
		if err != nil {
			return models.JobSpec{}, err
		}
		endDate = &end
	}

	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	for _, dep := range inputs.Dependencies {
		dependencies[dep] = models.JobSpecDependency{}
	}

	// prep window
	window, err := fac.prepareWindow(inputs)
	if err != nil {
		return models.JobSpec{}, err
	}

	job := models.JobSpec{
		Version: inputs.Version,
		Name:    strings.TrimSpace(inputs.Name),
		Owner:   inputs.Owner,
		Schedule: models.JobSpecSchedule{
			StartDate: startDate,
			EndDate:   endDate,
			Interval:  inputs.Schedule.Interval,
		},
		Behavior: models.JobSpecBehavior{
			Catchup:       true,
			DependsOnPast: false,
		},
		Task: models.JobSpecTask{
			Name:   inputs.Task.Name,
			Config: inputs.Task.Config,
			Window: window,
		},
		Asset:        inputs.Asset,
		Dependencies: dependencies,
	}
	return job, nil
}

func (fac *Factory) prepareWindow(in models.JobInput) (models.TaskWindow, error) {
	var err error
	window := models.TaskWindow{}
	window.Size = time.Hour * 24
	window.Offset = 0
	window.TruncateTo = "d"

	if in.Task.Window.TruncateTo != "" {
		window.TruncateTo = in.Task.Window.TruncateTo
	}
	if in.Task.Window.Size != "" {
		window.Size, err = time.ParseDuration(in.Task.Window.Size)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window of %s with size %v", in.Name, in.Task.Window.Size)
		}
	}
	if in.Task.Window.Offset != "" {
		window.Offset, err = time.ParseDuration(in.Task.Window.Offset)
		if err != nil {
			return window, errors.Wrapf(err, "failed to parse task window %s with offset %v", in.Name, in.Task.Window.Offset)
		}
	}
	return window, nil
}

func NewSpecFactory() *Factory {
	return &Factory{}
}
