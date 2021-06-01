package postgres

import (
	"encoding/json"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
)

type Instance struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;"`

	JobID uuid.UUID `gorm:"not null"`
	Job   Job       `gorm:"foreignKey:JobID;association_autoupdate:false"`

	ScheduledAt *time.Time `gorm:"not null"`
	State       string
	Data        datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt *time.Time
}

func (j Instance) ToSpec(job models.JobSpec) (models.InstanceSpec, error) {
	data := []models.InstanceSpecData{}
	if j.Data != nil {
		if err := json.Unmarshal(j.Data, &data); err != nil {
			return models.InstanceSpec{}, err
		}
	}

	var schdAt time.Time
	if j.ScheduledAt != nil {
		schdAt = *j.ScheduledAt
	}

	return models.InstanceSpec{
		ID:          j.ID,
		ScheduledAt: schdAt,
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

	var schdAt *time.Time = nil
	if !spec.ScheduledAt.IsZero() {
		schdAt = &spec.ScheduledAt
	}
	return Instance{
		ID:          spec.ID,
		ScheduledAt: schdAt,
		State:       spec.State,
		Data:        dataJSON,
		JobID:       job.ID,
	}, nil
}

type instanceRepository struct {
	db         *gorm.DB
	job        models.JobSpec
	jobAdapter *JobSpecAdapter

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
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find instance by schedule")
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
	var r Instance
	r.ID = existingJobSpecRun.ID
	return repo.db.Model(&r).Update(map[string]interface{}{"data": nil}).Error
}

func (repo *instanceRepository) GetByScheduledAt(scheduled time.Time) (models.InstanceSpec, error) {
	var r Instance
	if err := repo.db.Preload("Job").Where("job_id = ? AND scheduled_at = ?", repo.job.ID, scheduled).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.InstanceSpec{}, store.ErrResourceNotFound
		}
		return models.InstanceSpec{}, err
	}
	return r.ToSpec(repo.job)
}

func NewInstanceRepository(db *gorm.DB, job models.JobSpec, jobAdapter *JobSpecAdapter) *instanceRepository {
	return &instanceRepository{
		db:         db,
		job:        job,
		jobAdapter: jobAdapter,
	}
}
