package postgres

import (
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
	StartDate     time.Time
	EndDate       *time.Time
	Interval      string
	DependsOnPast *bool
	CatchUp       *bool
	Destination   string
	Dependencies  datatypes.JSON

	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

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

type JobTask struct {
	Name   string
	Config map[string]string

	WindowSize       time.Duration
	WindowOffset     time.Duration
	WindowTruncateTo string
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
	Config map[string]string
}

// ToSpec converts the postgres' JobHook representation to the optimus' models.JobSpecHook
func (a JobHook) ToSpec(supportedHookRepo models.SupportedHookRepo) (models.JobSpecHook, error) {
	hookUnit, err := supportedHookRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, errors.Wrap(err, "spec reading error")
	}
	return models.JobSpecHook{
		Config: a.Config,
		Unit:   hookUnit,
	}, nil
}

func (a JobHook) FromSpec(spec models.JobSpecHook) JobHook {
	return JobHook{
		Name:   spec.Unit.GetName(),
		Config: spec.Config,
	}
}

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
	if err := json.Unmarshal(conf.Dependencies, &dependencies); err != nil {
		return models.JobSpec{}, err
	}

	// prep task conf
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
func (adapt Adapter) FromSpec(spec models.JobSpec) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("task unit cannot be empty")
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
		hooks = append(hooks, JobHook{}.FromSpec(hook))
	}
	hooksJSON, err := json.Marshal(hooks)
	if err != nil {
		return Job{}, err
	}

	wsize := spec.Task.Window.Size.Nanoseconds()
	woffset := spec.Task.Window.Offset.Nanoseconds()

	jobDestination, err := spec.Task.Unit.GenerateDestination(models.UnitData{
		Config: spec.Task.Config,
		Assets: spec.Assets.ToMap(),
	})
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
		DependsOnPast:    &spec.Behavior.DependsOnPast,
		CatchUp:          &spec.Behavior.CatchUp,
		Destination:      jobDestination,
		Dependencies:     dependenciesJSON,
		TaskName:         spec.Task.Unit.GetName(),
		TaskConfig:       taskConfigJSON,
		WindowSize:       &wsize,
		WindowOffset:     &woffset,
		WindowTruncateTo: &spec.Task.Window.TruncateTo,
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
