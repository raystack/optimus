package postgres

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"gorm.io/datatypes"
	"github.com/odpf/optimus/models"
)

type Adapter struct {
	supportedTaskRepo models.SupportedTaskRepo
	supportedHookRepo models.SupportedHookRepo
}

func NewAdapter(supportedTaskRepo models.SupportedTaskRepo, supportedHookRepo models.SupportedHookRepo) *Adapter {
	return &Adapter{
		supportedTaskRepo: supportedTaskRepo,
		supportedHookRepo: supportedHookRepo,
	}
}

// ToSpec converts the postgres' Job representation to the optimus' JobSpec
func (adapt Adapter) ToSpec(conf Job) (models.JobSpec, error) {
	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	for _, dep := range conf.Dependencies {
		dependencies[dep] = models.JobSpecDependency{}
	}

	taskConf := map[string]string{}
	if err := json.Unmarshal(conf.TaskConfig, &taskConf); err != nil {
		return models.JobSpec{}, err
	}

	//prep assets
	jobAssets := []models.JobSpecAsset{}
	assetsRaw := []JobAsset{}
	if err := json.Unmarshal(conf.Assets, &assetsRaw); err != nil {
		return models.JobSpec{}, err
	}
	for _, asset := range assetsRaw {
		jobAssets = append(jobAssets, asset.ToSpec())
	}

	//prep hooks
	jobHooks := []models.JobSpecHook{}
	hooksRaw := []JobHook{}
	if err := json.Unmarshal(conf.Hooks, &hooksRaw); err != nil {
		return models.JobSpec{}, err
	}
	for _, hook := range hooksRaw {
		hookSpec, err := hook.ToSpec(adapt.supportedHookRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
		jobHooks = append(jobHooks, hookSpec)
	}

	execUnit, err := adapt.supportedTaskRepo.GetByName(conf.TaskName)
	if err != nil {
		return models.JobSpec{}, errors.Wrap(err, "spec reading error")
	}

	job := models.JobSpec{
		ID:      conf.ID,
		Version: conf.Version,
		Name:    conf.Name,
		Owner:   conf.Owner,
		Schedule: models.JobSpecSchedule{
			StartDate: conf.StartDate,
			EndDate:   conf.EndDate,
			Interval:  conf.Interval,
		},
		Behavior: models.JobSpecBehavior{
			CatchUp:       conf.CatchUp,
			DependsOnPast: conf.DependsOnPast,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConf,
			Window: models.JobSpecTaskWindow{
				Size:       time.Duration(conf.WindowSize),
				Offset:     time.Duration(conf.WindowOffset),
				TruncateTo: conf.WindowTruncateTo,
			},
		},
		Assets:       *(models.JobAssets{}).New(jobAssets),
		Dependencies: dependencies,
		Hooks:        jobHooks,
	}
	return job, nil
}

// FromSpec converts the optimus representation of JobSpec to postgres' Job
func (adapt Adapter) FromSpec(spec models.JobSpec) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("task unit cannot be empty")
	}

	dependencies := []string{}
	for dep := range spec.Dependencies {
		dependencies = append(dependencies, dep)
	}

	// prep task config
	taskConfigJSON, err := json.Marshal(spec.Task.Config)
	if err != nil {
		return Job{}, err
	}

	// prep assets
	assets := []JobAsset{}
	for _, jobAsset := range spec.Assets.GetAll() {
		assets = append(assets, JobAsset{}.FromSpec(jobAsset))
	}
	assetsJSON, err := json.Marshal(assets)
	if err != nil {
		return Job{}, err
	}

	hooks := []JobHook{}
	for _, hook := range spec.Hooks {
		hooks = append(hooks, JobHook{}.FromSpec(hook))
	}
	hooksJSON, err := json.Marshal(hooks)
	if err != nil {
		return Job{}, err
	}

	return Job{
		ID:               spec.ID,
		Version:          spec.Version,
		Name:             spec.Name,
		Owner:            spec.Owner,
		StartDate:        spec.Schedule.StartDate,
		EndDate:          spec.Schedule.EndDate,
		Interval:         spec.Schedule.Interval,
		DependsOnPast:    spec.Behavior.DependsOnPast,
		CatchUp:          spec.Behavior.CatchUp,
		Dependencies:     dependencies,
		TaskName:         spec.Task.Unit.GetName(),
		TaskConfig:       datatypes.JSON(taskConfigJSON),
		WindowSize:       spec.Task.Window.Size.Nanoseconds(),
		WindowOffset:     spec.Task.Window.Offset.Nanoseconds(),
		WindowTruncateTo: spec.Task.Window.TruncateTo,
		Assets:           assetsJSON,
		Hooks:            hooksJSON,
	}, nil
}

func (adapt Adapter) FromSpecWithProject(spec models.JobSpec, proj models.ProjectSpec) (Job, error) {
	adaptJob, err := adapt.FromSpec(spec)
	if err != nil {
		return adaptJob, err
	}
	adaptProject, err := Project{}.FromSpec(proj)
	if err != nil {
		return adaptJob, err
	}
	adaptJob.ProjectID = adaptProject.ID
	adaptJob.Project = adaptProject
	return adaptJob, nil
}
