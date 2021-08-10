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

	JobRunID uuid.UUID `gorm:"type:uuid"`
	JobRun   JobRun    `gorm:"foreignKey:JobRunID"`

	Name string `gorm:"column:instance_name;not null"`
	Type string `gorm:"column:instance_type;not null"`

	ExecutedAt *time.Time
	Status     string
	Data       datatypes.JSON

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (j Instance) ToSpec() (models.InstanceSpec, error) {
	var data []models.InstanceSpecData
	if j.Data != nil {
		if err := json.Unmarshal(j.Data, &data); err != nil {
			return models.InstanceSpec{}, err
		}
	}

	var execAt time.Time
	if j.ExecutedAt != nil {
		execAt = *j.ExecutedAt
	}

	return models.InstanceSpec{
		ID:         j.ID,
		Name:       j.Name,
		Type:       models.InstanceType(j.Type),
		ExecutedAt: execAt,
		Status:     models.JobRunState(j.Status),
		Data:       data,
		UpdatedAt:  j.UpdatedAt,
	}, nil
}

func (j Instance) FromSpec(spec models.InstanceSpec, jobRunID uuid.UUID) (Instance, error) {
	dataJSON, err := spec.DataToJSON()
	if err != nil {
		return Instance{}, err
	}

	var execAt *time.Time = nil
	if !spec.ExecutedAt.IsZero() {
		execAt = &spec.ExecutedAt
	}
	return Instance{
		ID:         spec.ID,
		JobRunID:   jobRunID,
		Name:       spec.Name,
		Type:       spec.Type.String(),
		ExecutedAt: execAt,
		Status:     spec.Status.String(),
		Data:       dataJSON,
	}, nil
}

type InstanceRepository struct {
	db         *gorm.DB
	jobAdapter *JobSpecAdapter

	Now func()
}

func (repo *InstanceRepository) Insert(run models.JobRun, spec models.InstanceSpec) error {
	resource, err := Instance{}.FromSpec(spec, run.ID)
	if err != nil {
		return err
	}
	return repo.db.Omit("JobRun").Create(&resource).Error
}

func (repo *InstanceRepository) Save(run models.JobRun, spec models.InstanceSpec) error {
	existingResource, err := repo.GetByName(run.ID, spec.Name, spec.Type.String())
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(run, spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find instance by schedule")
	}

	resource, err := Instance{}.FromSpec(spec, run.ID)
	if err != nil {
		return err
	}
	resource.ID = existingResource.ID
	return repo.db.Debug().Omit("JobRun").Model(&resource).Updates(&resource).Error
}

func (repo *InstanceRepository) UpdateStatus(id uuid.UUID, status models.JobRunState) error {
	var r Instance
	if err := repo.db.Where("id = ?", id).Find(&r).Error; err != nil {
		return err
	}
	r.Status = status.String()
	return repo.db.Omit("JobRun").Save(&r).Error
}

func (repo *InstanceRepository) GetByName(runID uuid.UUID, instanceName, instanceType string) (models.InstanceSpec, error) {
	var r Instance
	if err := repo.db.Preload("JobRun").Where("job_run_id = ? AND instance_name = ? AND instance_type = ?", runID, instanceName, instanceType).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.InstanceSpec{}, store.ErrResourceNotFound
		}
		return models.InstanceSpec{}, err
	}
	return r.ToSpec()
}

func (repo *InstanceRepository) GetByID(id uuid.UUID) (models.InstanceSpec, error) {
	var r Instance
	if err := repo.db.Preload("JobRun").Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.InstanceSpec{}, store.ErrResourceNotFound
		}
		return models.InstanceSpec{}, err
	}
	return r.ToSpec()
}

func (repo *InstanceRepository) Delete(id uuid.UUID) error {
	return repo.db.Where("id = ?", id).Delete(&Instance{}).Error
}

func (repo *InstanceRepository) DeleteByJobRun(runID uuid.UUID) error {
	return repo.db.Where("job_run_id = ?", runID).Delete(&Instance{}).Error
}

func NewInstanceRepository(db *gorm.DB, jobAdapter *JobSpecAdapter) *InstanceRepository {
	return &InstanceRepository{
		db:         db,
		jobAdapter: jobAdapter,
	}
}
