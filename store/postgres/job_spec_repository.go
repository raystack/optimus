package postgres

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
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
	DependsOnPast bool
	CatchUp       bool
	Dependencies  pq.StringArray

	ProjectID uuid.UUID
	Project   Project `gorm:"foreignKey:ProjectID"`

	TaskName         string
	TaskConfig       datatypes.JSON
	WindowSize       int64 //duration in nanos
	WindowOffset     int64
	WindowTruncateTo string

	Assets datatypes.JSON

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

type Adapter struct {
	supportedTaskRepo models.SupportedTaskRepo
}

func NewAdapter(supportedTaskRepo models.SupportedTaskRepo) *Adapter {
	return &Adapter{
		supportedTaskRepo: supportedTaskRepo,
	}
}

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
	}
	return job, nil
}

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
		return Job{}, nil
	}

	// prep assets
	assets := []JobAsset{}
	for _, jobAsset := range spec.Assets.GetAll() {
		assets = append(assets, JobAsset{}.FromSpec(jobAsset))
	}
	assetsJSON, err := json.Marshal(assets)
	if err != nil {
		return Job{}, nil
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

type jobSpecRepository struct {
	db      *gorm.DB
	project models.ProjectSpec
	adapter *Adapter
}

func (repo *jobSpecRepository) Insert(spec models.JobSpec) error {
	resource, err := repo.adapter.FromSpecWithProject(spec, repo.project)
	if err != nil {
		return err
	}
	if len(resource.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.Create(&resource).Error
}

func (repo *jobSpecRepository) Save(spec models.JobSpec) error {
	existingResource, err := repo.GetByName(spec.Name)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return repo.Insert(spec)
	}
	resource, err := repo.adapter.FromSpec(spec)
	if err != nil {
		return err
	}
	if err == nil {
		resource.ID = existingResource.ID
	}
	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *jobSpecRepository) GetByID(id uuid.UUID) (models.JobSpec, error) {
	var r Job
	if err := repo.db.Preload("Project").Where("id = ?", id).Find(&r).Error; err != nil {
		return models.JobSpec{}, err
	}
	return repo.adapter.ToSpec(r)
}

func (repo *jobSpecRepository) GetByName(name string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.Preload("Project").Where("project_id = ? AND name = ?", repo.project.ID, name).Find(&r).Error; err != nil {
		return models.JobSpec{}, err
	}
	return repo.adapter.ToSpec(r)
}

func (repo *jobSpecRepository) GetAll() ([]models.JobSpec, error) {
	specs := []models.JobSpec{}
	jobs := []Job{}
	if err := repo.db.Find(&jobs).Error; err != nil {
		return specs, err
	}
	for _, job := range jobs {
		adapt, err := repo.adapter.ToSpec(job)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func NewJobRepository(db *gorm.DB, project models.ProjectSpec, adapter *Adapter) *jobSpecRepository {
	return &jobSpecRepository{
		db:      db,
		project: project,
		adapter: adapter,
	}
}
