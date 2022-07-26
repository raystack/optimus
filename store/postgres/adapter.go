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

	WindowSize       *string
	WindowOffset     *string
	WindowTruncateTo *string

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

	var window models.Window
	switch conf.Version {
	case 1:
		var truncateTo string
		if conf.WindowTruncateTo != nil {
			truncateTo = *conf.WindowTruncateTo
		}

		var offset string
		if conf.WindowOffset != nil {
			offset = *conf.WindowOffset
		} else if conf.OldWindowOffset != nil {
			offset = fmt.Sprintf("%fh", time.Duration(*conf.OldWindowOffset).Hours())
		}

		var size string
		if conf.WindowSize != nil {
			size = *conf.WindowSize
		} else if conf.OldWindowSize != nil {
			size = fmt.Sprintf("%fh", time.Duration(*conf.OldWindowSize).Hours())
		}

		window = models.WindowV1{
			TruncateTo: truncateTo,
			Offset:     offset,
			Size:       size,
		}
	case 2:
		var truncateTo string
		if conf.WindowTruncateTo != nil {
			truncateTo = *conf.WindowTruncateTo
		}

		var offset string
		if conf.WindowOffset != nil {
			offset = *conf.WindowOffset
		}

		var size string
		if conf.WindowSize != nil {
			size = *conf.WindowSize
		}

		window = models.WindowV2{
			TruncateTo: truncateTo,
			Offset:     offset,
			Size:       size,
		}
	default:
		return models.JobSpec{}, fmt.Errorf("spec version [%d] is not recognized", conf.Version)
	}

	job := models.JobSpec{
		ID:                  conf.ID,
		Version:             conf.Version,
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

	var truncateTo, offset, size *string
	var oldSize, oldOffset *time.Duration
	if spec.Task.Window != nil {
		tempTruncateTo := spec.Task.Window.GetTruncateTo()
		if tempTruncateTo != "" {
			truncateTo = &tempTruncateTo
		}
		tempSize := spec.Task.Window.GetSize()
		if tempSize != "" {
			size = &tempSize
		}
		if spec.Task.Window.GetSizeAsDuration() > 0 {
			tempSizeDuration := spec.Task.Window.GetSizeAsDuration()
			oldSize = &tempSizeDuration
		}
		tempOffset := spec.Task.Window.GetOffset()
		if tempOffset != "" {
			offset = &tempOffset
		}
		if spec.Task.Window.GetOffsetAsDuration() > 0 {
			tempOffsetDuration := spec.Task.Window.GetOffsetAsDuration()
			oldOffset = &tempOffsetDuration
		}
	}

	metadata, err := json.Marshal(spec.Metadata)
	if err != nil {
		return Job{}, err
	}

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
		OldWindowSize:        (*int64)(oldSize),
		OldWindowOffset:      (*int64)(oldOffset),
	}, nil
}

func (adapt JobSpecAdapter) FromSpecWithNamespace(spec models.JobSpec, namespace models.NamespaceSpec, jobDestination string) (Job, error) {
	adaptJob, err := adapt.FromJobSpec(spec, jobDestination)
	if err != nil {
		return adaptJob, err
	}

	// namespace
	adaptNamespace := Namespace{}.FromSpecWithProject(namespace, namespace.ProjectSpec)

	adaptJob.NamespaceID = adaptNamespace.ID
	adaptJob.Namespace = adaptNamespace

	// project
	adaptProject := Project{}.FromSpec(namespace.ProjectSpec)

	adaptJob.ProjectID = adaptProject.ID
	adaptJob.Project = adaptProject

	return adaptJob, nil
}

type JobRun struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobID uuid.UUID

	// job spec for which this run was created, spec should contain a valid
	// uuid if it belongs to a saved job and not an adhoc job
	Spec datatypes.JSON `gorm:"column:specification;"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	Trigger     string
	Status      string
	ScheduledAt time.Time
	Data        datatypes.JSON

	Instances []Instance

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

// TableName overrides the table name used by JobRun to `job_run_old`
func (JobRun) TableName() string {
	return "job_run_old"
}

type JobRunData struct {
	ExecutedAt time.Time
}

func (adapt JobSpecAdapter) FromJobRun(jr models.JobRun, nsSpec models.NamespaceSpec, jobDestination string) (JobRun, error) {
	adaptedJobSpec, err := adapt.FromJobSpec(jr.Spec, jobDestination)
	if err != nil {
		return JobRun{}, err
	}
	specBytes, err := json.Marshal(adaptedJobSpec)
	if err != nil {
		return JobRun{}, err
	}

	dataBytes, err := json.Marshal(JobRunData{
		ExecutedAt: jr.ExecutedAt,
	})
	if err != nil {
		return JobRun{}, err
	}

	// namespace
	adaptNamespace := Namespace{}.FromSpecWithProject(nsSpec, nsSpec.ProjectSpec)

	var instances []Instance
	for _, instanceSpec := range jr.Instances {
		instance, err := Instance{}.FromSpec(instanceSpec, jr.ID)
		if err != nil {
			return JobRun{}, err
		}
		instances = append(instances, instance)
	}

	return JobRun{
		ID:    jr.ID,
		JobID: jr.Spec.ID,
		Spec:  specBytes,

		NamespaceID: adaptNamespace.ID,
		Namespace:   adaptNamespace,

		Trigger:     jr.Trigger.String(),
		Status:      jr.Status.String(),
		ScheduledAt: jr.ScheduledAt,
		Data:        dataBytes,

		Instances: instances,
	}, nil
}

func (adapt JobSpecAdapter) ToJobRun(jr JobRun) (models.JobRun, models.NamespaceSpec, error) {
	adaptedSpec := Job{}
	if err := json.Unmarshal(jr.Spec, &adaptedSpec); err != nil {
		return models.JobRun{}, models.NamespaceSpec{}, err
	}

	jobSpec, err := adapt.ToSpec(adaptedSpec)
	if err != nil {
		return models.JobRun{}, models.NamespaceSpec{}, err
	}

	adaptedData := JobRunData{}
	if len(jr.Data) != 0 {
		if err := json.Unmarshal(jr.Data, &adaptedData); err != nil {
			return models.JobRun{}, models.NamespaceSpec{}, err
		}
	} else {
		// to make it backward compatible, generate execution time
		// although this time may not match exactly what it should be
		// but will avoid failing
		adaptedData.ExecutedAt = jr.ScheduledAt
	}

	var instanceSpecs []models.InstanceSpec
	for _, instance := range jr.Instances {
		is, err := instance.ToSpec()
		if err != nil {
			return models.JobRun{}, models.NamespaceSpec{}, err
		}
		instanceSpecs = append(instanceSpecs, is)
	}

	var adaptProject models.ProjectSpec
	var adaptNamespace models.NamespaceSpec

	if jr.Namespace.Name != "" {
		adaptProject = jr.Namespace.Project.ToSpec()

		// namespace
		adaptNamespace, err = jr.Namespace.ToSpec(adaptProject)
		if err != nil {
			return models.JobRun{}, models.NamespaceSpec{}, err
		}
	}

	return models.JobRun{
		ID:          jr.ID,
		Spec:        jobSpec,
		Trigger:     models.JobRunTrigger(jr.Trigger),
		Status:      models.JobRunState(jr.Status),
		ScheduledAt: jr.ScheduledAt,
		Instances:   instanceSpecs,
		ExecutedAt:  adaptedData.ExecutedAt,
	}, adaptNamespace, nil
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
