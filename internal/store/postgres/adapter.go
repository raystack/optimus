package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

// Job are inputs from user to create a job
// postgres representation of the job
type Job struct {
	ID           uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	Version      int
	Name         string `gorm:"not null" json:"name"`
	Owner        string
	Description  string
	Labels       datatypes.JSON
	StartDate    time.Time
	EndDate      *time.Time
	Interval     *string
	Destination  string
	Dependencies datatypes.JSON
	Behavior     datatypes.JSON

	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	TaskName   string
	TaskConfig datatypes.JSON

	WindowSize       string
	WindowOffset     string
	WindowTruncateTo string

	Assets               datatypes.JSON
	Hooks                datatypes.JSON
	Metadata             datatypes.JSON
	ExternalDependencies datatypes.JSON // store external dependencies

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt

	// Deprecated: do not use it unless WindowSize is empty
	OldWindowSize *int64 // duration in nanos
	// Deprecated: do not use it unless WindowOffset is empty
	OldWindowOffset *int64
}

type JobBehavior struct {
	DependsOnPast bool
	CatchUp       bool
	Retry         JobBehaviorRetry
	Notify        []JobBehaviorNotifier
}

type JobBehaviorRetry struct {
	Count              int
	Delay              int64
	ExponentialBackoff bool
}

type JobBehaviorNotifier struct {
	On       string
	Config   map[string]string
	Channels []string
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

func (JobAsset) FromSpec(spec models.JobSpecAsset) JobAsset {
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
func (a JobHook) ToSpec(pluginRepo models.PluginRepository) (models.JobSpecHook, error) {
	hookUnit, err := pluginRepo.GetByName(a.Name)
	if err != nil {
		return models.JobSpecHook{}, fmt.Errorf("spec reading error: %w", err)
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

func (JobHook) FromSpec(spec models.JobSpecHook) (JobHook, error) {
	configJSON, err := json.Marshal(spec.Config)
	if err != nil {
		return JobHook{}, err
	}
	return JobHook{
		Name:   spec.Unit.Info().Name,
		Config: configJSON,
	}, nil
}

type JobSpecAdapter struct {
	pluginRepo models.PluginRepository
}

func NewAdapter(pluginRepo models.PluginRepository) *JobSpecAdapter {
	return &JobSpecAdapter{
		pluginRepo: pluginRepo,
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

	behavior := JobBehavior{}
	if conf.Behavior != nil {
		if err := json.Unmarshal(conf.Behavior, &behavior); err != nil {
			return models.JobSpec{}, err
		}
	}

	// prep dirty dependencies
	dependencies := map[string]models.JobSpecDependency{}
	if err := json.Unmarshal(conf.Dependencies, &dependencies); err != nil {
		return models.JobSpec{}, err
	}

	// prep external dependencies
	externalDependencies := models.ExternalDependency{}
	if conf.ExternalDependencies != nil {
		if err := json.Unmarshal(conf.ExternalDependencies, &externalDependencies); err != nil {
			return models.JobSpec{}, err
		}
	}
	// prep task conf
	taskConf := models.JobSpecConfigs{}
	if err := json.Unmarshal(conf.TaskConfig, &taskConf); err != nil {
		return models.JobSpec{}, err
	}

	// prep assets
	jobAssets := []models.JobSpecAsset{}
	assetsRaw := []JobAsset{}
	if err := json.Unmarshal(conf.Assets, &assetsRaw); err != nil {
		return models.JobSpec{}, err
	}
	for _, asset := range assetsRaw {
		jobAssets = append(jobAssets, asset.ToSpec())
	}

	// prep hooks
	jobHooks := []models.JobSpecHook{}
	hooksRaw := []JobHook{}
	if err := json.Unmarshal(conf.Hooks, &hooksRaw); err != nil {
		return models.JobSpec{}, err
	}
	for _, hook := range hooksRaw {
		hookSpec, err := hook.ToSpec(adapt.pluginRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
		jobHooks = append(jobHooks, hookSpec)
	}

	execUnit, err := adapt.pluginRepo.GetByName(conf.TaskName)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("spec reading error: %w", err)
	}

	var notifiers []models.JobSpecNotifier
	for _, notify := range behavior.Notify {
		notifiers = append(notifiers, models.JobSpecNotifier{
			On:       models.JobEventType(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	var metadata models.JobSpecMetadata
	if conf.Metadata != nil {
		if err := json.Unmarshal(conf.Metadata, &metadata); err != nil {
			return models.JobSpec{}, err
		}
	}

	namespaceSpec, err := conf.Namespace.ToSpec(conf.Project.ToSpec())
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("getting namespace spec of a job error: %w", err)
	}

	var offset, size = conf.WindowOffset, conf.WindowSize
	if offset == "" && conf.OldWindowOffset != nil {
		offset = fmt.Sprintf("%fh", time.Duration(*conf.OldWindowOffset).Hours())
	}
	if size == "" && conf.OldWindowSize != nil {
		size = fmt.Sprintf("%fh", time.Duration(*conf.OldWindowSize).Hours())
	}

	window, err := models.NewWindow(conf.Version, conf.WindowTruncateTo, offset, size)
	if err != nil {
		return models.JobSpec{}, err
	}
	if err := window.Validate(); err != nil {
		return models.JobSpec{}, err
	}

	version := conf.Version
	if version == 0 {
		version = models.JobSpecDefaultVersion
	}
	job := models.JobSpec{
		ID:                  conf.ID,
		Version:             version,
		Name:                conf.Name,
		Owner:               conf.Owner,
		ResourceDestination: conf.Destination,
		Description:         conf.Description,
		Labels:              labels,
		Schedule: models.JobSpecSchedule{
			StartDate: conf.StartDate,
			EndDate:   conf.EndDate,
			Interval:  *conf.Interval,
		},
		Behavior: models.JobSpecBehavior{
			DependsOnPast: behavior.DependsOnPast,
			CatchUp:       behavior.CatchUp,
			Retry: models.JobSpecBehaviorRetry{
				Count:              behavior.Retry.Count,
				Delay:              time.Duration(behavior.Retry.Delay),
				ExponentialBackoff: behavior.Retry.ExponentialBackoff,
			},
			Notify: notifiers,
		},
		Task: models.JobSpecTask{
			Unit:   execUnit,
			Config: taskConf,
			Window: window,
		},
		Assets:               *(models.JobAssets{}).New(jobAssets),
		Dependencies:         dependencies,
		Hooks:                jobHooks,
		Metadata:             metadata,
		ExternalDependencies: externalDependencies,
		NamespaceSpec:        namespaceSpec,
	}
	return job, nil
}

// FromJobSpec converts the optimus representation of JobSpec to postgres' Job
func (JobSpecAdapter) FromJobSpec(spec models.JobSpec, resourceDestination string) (Job, error) {
	if spec.Task.Unit == nil {
		return Job{}, errors.New("task unit cannot be empty")
	}

	labelsJSON, err := json.Marshal(spec.Labels)
	if err != nil {
		return Job{}, err
	}

	var notifiers []JobBehaviorNotifier
	for _, notify := range spec.Behavior.Notify {
		notifiers = append(notifiers, JobBehaviorNotifier{
			On:       string(notify.On),
			Config:   notify.Config,
			Channels: notify.Channels,
		})
	}

	behaviorJSON, err := json.Marshal(JobBehavior{
		DependsOnPast: spec.Behavior.DependsOnPast,
		CatchUp:       spec.Behavior.CatchUp,
		Retry: JobBehaviorRetry{
			Count:              spec.Behavior.Retry.Count,
			Delay:              spec.Behavior.Retry.Delay.Nanoseconds(),
			ExponentialBackoff: spec.Behavior.Retry.ExponentialBackoff,
		},
		Notify: notifiers,
	})
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

	// prep external dependencies
	externalDependenciesJSON, err := json.Marshal(spec.ExternalDependencies)
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

	metadata, err := json.Marshal(spec.Metadata)
	if err != nil {
		return Job{}, err
	}

	var truncateTo, offset, size string
	if spec.Task.Window != nil {
		truncateTo = spec.Task.Window.GetTruncateTo()
		offset = spec.Task.Window.GetOffset()
		size = spec.Task.Window.GetSize()
	}
	project := Project{}.FromSpec(spec.GetProjectSpec())
	namespace := Namespace{}.FromSpec(spec.NamespaceSpec)
	return Job{
		ID:                   spec.ID,
		Version:              spec.Version,
		Name:                 spec.Name,
		Owner:                spec.Owner,
		Description:          spec.Description,
		Labels:               labelsJSON,
		StartDate:            spec.Schedule.StartDate,
		EndDate:              spec.Schedule.EndDate,
		Interval:             &spec.Schedule.Interval,
		Behavior:             behaviorJSON,
		Destination:          resourceDestination,
		Dependencies:         dependenciesJSON,
		TaskName:             spec.Task.Unit.Info().Name,
		TaskConfig:           taskConfigJSON,
		WindowSize:           size,
		WindowOffset:         offset,
		WindowTruncateTo:     truncateTo,
		Assets:               assetsJSON,
		Hooks:                hooksJSON,
		Metadata:             metadata,
		ExternalDependencies: externalDependenciesJSON,
		ProjectID:            project.ID,
		Project:              project,
		NamespaceID:          namespace.ID,
		Namespace:            namespace,
	}, nil
}

type jobDependency struct {
	JobID uuid.UUID `json:"job_id"`

	DependencyID          uuid.UUID `json:"dependency_id"`
	DependencyName        string    `json:"dependency_name"`
	DependencyTaskName    string    `json:"dependency_task_name"`
	DependencyDestination string    `json:"dependency_destination"`

	DependencyProjectID uuid.UUID `json:"dependency_project_id"`
	DependencyProject   Project   `gorm:"foreignKey:DependencyProjectID"`

	DependencyNamespaceID uuid.UUID `json:"dependency_namespace_id"`
	DependencyNamespace   Namespace `gorm:"foreignKey:DependencyNamespaceID"`
}

func (adapt JobSpecAdapter) groupToDependenciesPerJobID(jobDependencies []jobDependency) (map[uuid.UUID][]models.JobSpec, error) {
	jobDependenciesByJobID := make(map[uuid.UUID][]models.JobSpec)
	for _, dependency := range jobDependencies {
		dependencyJobSpec, err := adapt.dependencyToJobSpec(dependency)
		if err != nil {
			return nil, err
		}
		jobDependenciesByJobID[dependency.JobID] = append(jobDependenciesByJobID[dependency.JobID], dependencyJobSpec)
	}
	return jobDependenciesByJobID, nil
}

// dependencyToJobSpec converts the postgres' JobDependency representation to the optimus' JobSpec
func (adapt JobSpecAdapter) dependencyToJobSpec(conf jobDependency) (models.JobSpec, error) {
	namespaceSpec, err := conf.DependencyNamespace.ToSpec(conf.DependencyProject.ToSpec())
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("getting namespace spec of a job error: %w", err)
	}

	execUnit, err := adapt.pluginRepo.GetByName(conf.DependencyTaskName)
	if err != nil {
		return models.JobSpec{}, fmt.Errorf("spec reading error: %w", err)
	}

	job := models.JobSpec{
		ID:                  conf.DependencyID,
		Name:                conf.DependencyName,
		ResourceDestination: conf.DependencyDestination,
		Task:                models.JobSpecTask{Unit: execUnit},
		NamespaceSpec:       namespaceSpec,
	}
	return job, nil
}
