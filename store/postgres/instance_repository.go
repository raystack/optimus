package postgres

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
	"github.com/odpf/optimus/models"
)

type Instance struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;"`

	JobID uuid.UUID `gorm:"not null"`
	Job   Job       `gorm:"foreignKey:JobID;association_autoupdate:false"`

	ScheduledAt time.Time
	State       string
	Data        datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt *time.Time
}

func (j Instance) ToSpec(job models.JobSpec) (models.InstanceSpec, error) {
	data := []models.InstanceSpecData{}
	if err := json.Unmarshal(j.Data, &data); err != nil {
		return models.InstanceSpec{}, err
	}

	return models.InstanceSpec{
		ID:          j.ID,
		ScheduledAt: j.ScheduledAt,
		State:       j.State,
		Data:        data,
		Job:         job,
	}, nil
}

func (j Instance) FromSpec(spec models.InstanceSpec, job Job) (Instance, error) {
	dataJSON, err := spec.DataToJSON()
	if err != nil {
		return Instance{}, err
	}
	return Instance{
		ID:          spec.ID,
		ScheduledAt: spec.ScheduledAt,
		State:       spec.State,
		Data:        datatypes.JSON(dataJSON),
		JobID:       job.ID,
	}, nil
}

type instanceRepository struct {
	db         *gorm.DB
	job        models.JobSpec
	jobAdapter *Adapter

	Now func()
}

func (repo *instanceRepository) Insert(spec models.InstanceSpec) error {
	job, err := repo.jobAdapter.FromSpec(repo.job)
	if err != nil {
		return err
	}
	resource, err := Instance{}.FromSpec(spec, job)
	if err != nil {
		return err
	}
	return repo.db.Create(&resource).Error
}

func (repo *instanceRepository) Save(spec models.InstanceSpec) error {
	existingResource, err := repo.GetByScheduledAt(spec.ScheduledAt)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return repo.Insert(spec)
	}

	job, err := repo.jobAdapter.FromSpec(repo.job)
	if err != nil {
		return err
	}
	resource, err := Instance{}.FromSpec(spec, job)
	if err == nil {
		resource.ID = existingResource.ID
	}

	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *instanceRepository) Clear(scheduled time.Time) error {
	existingJobSpecRun, err := repo.GetByScheduledAt(scheduled)
	if err != nil && err != gorm.ErrRecordNotFound {
		return err
	}
	// TODO: check if existinJobSpecRun.Data is set to nil by itself since it's a pointer.
	return repo.db.Model(&existingJobSpecRun).Update("data = ?", nil).Error
}

func (repo *instanceRepository) GetByScheduledAt(scheduled time.Time) (models.InstanceSpec, error) {
	var r Instance
	if err := repo.db.Preload("Job").Where("job_id = ? AND scheduled_at = ?", repo.job.ID, scheduled).Find(&r).Error; err != nil {
		return models.InstanceSpec{}, err
	}
	return r.ToSpec(repo.job)
}

func NewInstanceRepository(db *gorm.DB, job models.JobSpec, jobAdapter *Adapter) *instanceRepository {
	return &instanceRepository{
		db:         db,
		job:        job,
		jobAdapter: jobAdapter,
	}
}
