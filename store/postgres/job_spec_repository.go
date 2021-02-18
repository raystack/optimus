package postgres

import (
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
	Type   string
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
		Type:   a.Type,
		Unit:   hookUnit,
	}, nil
}

func (a JobHook) FromSpec(spec models.JobSpecHook) JobHook {
	return JobHook{
		Name:   spec.Unit.GetName(),
		Config: spec.Config,
		Type:   spec.Type,
	}
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
	resource.ID = existingResource.ID
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

func (repo *jobSpecRepository) Delete(name string) error {
	return repo.db.Where("project_id = ? AND name = ?", repo.project.ID, name).Delete(&Job{}).Error
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
