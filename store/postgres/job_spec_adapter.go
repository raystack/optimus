package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
	"github.com/odpf/optimus/models"
)

// Job are inputs from user to create a job
// postgres representation of the job
type Job struct {
	ID            uuid.UUID `gorm:"primary_key;type:uuid;"`
	Version       int
	Name          string `gorm:"not null" json:"name"`
	Owner         string
	Description   string
	Labels        datatypes.JSON
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
	DependsOnPast *bool
	CatchUp       *bool
	Destination   string
	Dependencies  datatypes.JSON

	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	TaskName         string
	TaskConfig       datatypes.JSON
	WindowSize       *int64 //duration in nanos
	WindowOffset     *int64
	WindowTruncateTo *string

	Assets datatypes.JSON
	Hooks  datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt *time.Time
}

type JobAsset struct {
	Name  string
	Value string
}

func (a JobAsset) ToSpec() models.JobSpecAsset {
	return models.JobSpecAsset{
		Name:  a.Name,
		Value: a.Value,
	}
}

func (a JobAsset) FromSpec(spec models.JobSpecAsset) JobAsset {
	return JobAsset{
		Name:  spec.Name,
		Value: spec.Value,
	}
}

type JobHook struct {
	Name   string
	Config datatypes.JSON
}

// ToSpec converts the postgres' JobHook representation to the optimus' models.JobSpecHook
func (a JobHook) ToSpec(supportedHookRepo models.HookRepo) (models.JobSpecHook, error) {
	hookUnit, err := supportedHookRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, errors.Wrap(err, "spec reading error")
	}

	conf := models.JobSpecConfigs{}
	if err := json.Unmarshal(a.Config, &conf); err != nil {
		return models.JobSpecHook{}, err
	}

	return models.JobSpecHook{
		Config: conf,
		Unit:   hookUnit,
	}, nil
}

func (a JobHook) FromSpec(spec models.JobSpecHook) (JobHook, error) {
	configJSON, err := json.Marshal(spec.Config)
	if err != nil {
		return JobHook{}, err
	}
	schema, err := spec.Unit.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
	if err != nil {
		return JobHook{}, err
	}
	return JobHook{
		Name:   schema.Name,
		Config: configJSON,
	}, nil
}

type JobSpecAdapter struct {
	supportedTaskRepo models.TaskPluginRepository
	supportedHookRepo models.HookRepo
}

func NewAdapter(supportedTaskRepo models.TaskPluginRepository, supportedHookRepo models.HookRepo) *JobSpecAdapter {
	return &JobSpecAdapter{
		supportedTaskRepo: supportedTaskRepo,
		supportedHookRepo: supportedHookRepo,
	}
}

// ToSpec converts the postgres' Job representation to the optimus' JobSpec
func (adapt JobSpecAdapter) ToSpec(conf Job) (models.JobSpec, error) {

	labels := map[string]string{}
	if conf.Labels != nil {
		if err := json.Unmarshal(conf.Labels, &labels); err != nil {
			return models.JobSpec{}, err
		}
	}

	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	if err := json.Unmarshal(conf.Dependencies, &dependencies); err != nil {
		return models.JobSpec{}, err
	}

	// prep task conf
	taskConf := models.JobSpecConfigs{}
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
		ID:          conf.ID,
		Version:     conf.Version,
		Name:        conf.Name,
		Owner:       conf.Owner,
		Description: conf.Description,
		Labels:      labels,
		Schedule: models.JobSpecSchedule{
			StartDate: conf.StartDate,
			EndDate:   conf.EndDate,
			Interval:  conf.Interval,
		},
		Behavior: models.JobSpecBehavior{
			CatchUp:       *conf.CatchUp,
			DependsOnPast: *conf.DependsOnPast,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConf,
			Window: models.JobSpecTaskWindow{
				Size:       time.Duration(*conf.WindowSize),
				Offset:     time.Duration(*conf.WindowOffset),
				TruncateTo: *conf.WindowTruncateTo,
			},
		},
		Assets:       *(models.JobAssets{}).New(jobAssets),
		Dependencies: dependencies,
		Hooks:        jobHooks,
	}
	return job, nil
}

// FromSpec converts the optimus representation of JobSpec to postgres' Job
func (adapt JobSpecAdapter) FromSpec(spec models.JobSpec) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("task unit cannot be empty")
	}

	labelsJSON, err := json.Marshal(spec.Labels)
	if err != nil {
		return Job{}, err
	}

	// prep dependencies, make them dirty first(remove job and project)
	for idx, dep := range spec.Dependencies {
		dep.Project = nil
		dep.Job = nil
		spec.Dependencies[idx] = dep
	}
	dependenciesJSON, err := json.Marshal(spec.Dependencies)
	if err != nil {
		return Job{}, err
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
		h, err := JobHook{}.FromSpec(hook)
		if err != nil {
			return Job{}, err
		}
		hooks = append(hooks, h)
	}
	hooksJSON, err := json.Marshal(hooks)
	if err != nil {
		return Job{}, err
	}

	wsize := spec.Task.Window.Size.Nanoseconds()
	woffset := spec.Task.Window.Offset.Nanoseconds()

	taskSchema, err := spec.Task.Unit.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
	if err != nil {
		return Job{}, err
	}

	jobDestination, err := spec.Task.Unit.GenerateTaskDestination(context.TODO(), models.GenerateTaskDestinationRequest{
		Config: models.TaskPluginConfigs{}.FromJobSpec(spec.Task.Config),
		Assets: models.TaskPluginAssets{}.FromJobSpec(spec.Assets),
	})
	if err != nil {
		return Job{}, err
	}

	return Job{
		ID:               spec.ID,
		Version:          spec.Version,
		Name:             spec.Name,
		Owner:            spec.Owner,
		Description:      spec.Description,
		Labels:           labelsJSON,
		StartDate:        spec.Schedule.StartDate,
		EndDate:          spec.Schedule.EndDate,
		Interval:         spec.Schedule.Interval,
		DependsOnPast:    &spec.Behavior.DependsOnPast,
		CatchUp:          &spec.Behavior.CatchUp,
		Destination:      jobDestination.Destination,
		Dependencies:     dependenciesJSON,
		TaskName:         taskSchema.Name,
		TaskConfig:       taskConfigJSON,
		WindowSize:       &wsize,
		WindowOffset:     &woffset,
		WindowTruncateTo: &spec.Task.Window.TruncateTo,
		Assets:           assetsJSON,
		Hooks:            hooksJSON,
	}, nil
}

func (adapt JobSpecAdapter) FromSpecWithNamespace(spec models.JobSpec, namespace models.NamespaceSpec) (Job, error) {
	adaptJob, err := adapt.FromSpec(spec)
	if err != nil {
		return adaptJob, err
	}

	// namespace
	adaptNamespace, err := Namespace{}.FromSpecWithProject(namespace, namespace.ProjectSpec)
	if err != nil {
		return adaptJob, err
	}
	adaptJob.NamespaceID = adaptNamespace.ID
	adaptJob.Namespace = adaptNamespace

	// project
	adaptProject, err := Project{}.FromSpec(namespace.ProjectSpec)
	if err != nil {
		return adaptJob, err
	}
	adaptJob.ProjectID = adaptProject.ID
	adaptJob.Project = adaptProject

	return adaptJob, nil
}
