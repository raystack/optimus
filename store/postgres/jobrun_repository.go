package postgres

import (
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
)

type JobRunRepository struct {
	db           *gorm.DB
	adapter      *JobSpecAdapter
	instanceRepo *InstanceRepository
}

func (repo *JobRunRepository) Insert(namespace models.NamespaceSpec, spec models.JobRun) error {
	resource, err := repo.adapter.FromJobRun(spec, namespace)
	if err != nil {
		return err
	}
	return repo.db.Omit("Namespace").Create(&resource).Error
}

func (repo *JobRunRepository) Save(namespace models.NamespaceSpec, spec models.JobRun) error {
	if spec.Status == "" {
		// mark default state pending
		spec.Status = models.RunStatePending
	}

	existingResource, _, err := repo.GetByID(spec.ID)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(namespace, spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find jobrun by id")
	}

	resource, err := repo.adapter.FromJobRun(spec, namespace)
	if err != nil {
		return err
	}
	resource.ID = existingResource.ID
	return repo.db.Omit("Namespace").Model(&resource).Updates(&resource).Error
}

func (repo *JobRunRepository) GetByID(id uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	var r JobRun
	if err := repo.db.Preload("Namespace").Preload("Instances").Where("id = ?", id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.JobRun{}, models.NamespaceSpec{}, err
	}
	return repo.adapter.ToJobRun(r)
}

func (repo *JobRunRepository) GetByScheduledAt(jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error) {
	var r JobRun
	if err := repo.db.Preload("Namespace").Preload("Instances").Where("job_id = ? AND scheduled_at = ?", jobID, scheduledAt).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.JobRun{}, models.NamespaceSpec{}, err
	}
	return repo.adapter.ToJobRun(r)
}

// AddInstance associate instance details
func (repo *JobRunRepository) AddInstance(namespaceSpec models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error {
	for idx, instance := range run.Instances {
		if instance.Name == spec.Name && instance.Type == spec.Type {
			// delete if associated before
			if err := repo.instanceRepo.Delete(instance.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}

			// delete this index
			run.Instances[idx] = run.Instances[len(run.Instances)-1]
			run.Instances = run.Instances[:len(run.Instances)-1]
			break
		}
	}
	run.Instances = append(run.Instances, spec)
	return repo.Save(namespaceSpec, run)
}

// ClearInstances deletes all associated instance details
func (repo *JobRunRepository) ClearInstances(jobID uuid.UUID, scheduled time.Time) error {
	var r JobRun
	if err := repo.db.Where("job_id = ? AND scheduled_at = ?", jobID, scheduled).Find(&r).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	if err := repo.instanceRepo.DeleteByJobRun(r.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return repo.db.Model(&r).Update(map[string]interface{}{"data": nil, "status": models.RunStatePending}).Error
}

// ClearInstance deletes associated instance details
func (repo *JobRunRepository) ClearInstance(runID uuid.UUID, instanceType models.InstanceType, instanceName string) error {
	r, _, err := repo.GetByID(runID)
	if err != nil {
		return err
	}
	for _, instance := range r.Instances {
		if instance.Name == instanceName && instance.Type == instanceType {
			if err := repo.instanceRepo.Delete(instance.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			break
		}
	}
	return nil
}

// Clear prepares job run for fresh start
func (repo *JobRunRepository) Clear(runID uuid.UUID) error {
	r, _, err := repo.GetByID(runID)
	if err != nil {
		return err
	}
	if err := repo.instanceRepo.DeleteByJobRun(runID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return repo.db.Model(&r).Update(map[string]interface{}{"data": nil, "status": models.RunStatePending}).Error
}

func (repo *JobRunRepository) Delete(id uuid.UUID) error {
	if err := repo.instanceRepo.DeleteByJobRun(id); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return repo.db.Where("id = ?", id).Delete(&JobRun{}).Error
}

func (repo *JobRunRepository) UpdateStatus(id uuid.UUID, status models.JobRunState) error {
	var jr JobRun
	if err := repo.db.Where("id = ?", id).Find(&jr).Error; err != nil {
		return err
	}
	jr.Status = status.String()
	return repo.db.Omit("Namespace").Save(jr).Error
}

func (repo *JobRunRepository) GetByStatus(statuses ...models.JobRunState) ([]models.JobRun, error) {
	var specs []models.JobRun
	var runs []JobRun
	if err := repo.db.Preload("Instances").Where("status IN (?)", statuses).Find(&runs).Error; err != nil {
		return specs, err
	}

	for _, run := range runs {
		adapt, _, err := repo.adapter.ToJobRun(run)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func (repo *JobRunRepository) GetByTrigger(trigger models.JobRunTrigger, statuses ...models.JobRunState) ([]models.JobRun, error) {
	var specs []models.JobRun
	var runs []JobRun
	if len(statuses) > 0 {
		if err := repo.db.Preload("Instances").Where("trigger = ? and status IN (?)", trigger, statuses).Find(&runs).Error; err != nil {
			return specs, err
		}
	} else {
		if err := repo.db.Preload("Instances").Where("trigger = ?", trigger).Find(&runs).Error; err != nil {
			return specs, err
		}
	}

	for _, run := range runs {
		adapt, _, err := repo.adapter.ToJobRun(run)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func NewJobRunRepository(db *gorm.DB, adapter *JobSpecAdapter) *JobRunRepository {
	return &JobRunRepository{
		db:           db,
		adapter:      adapter,
		instanceRepo: NewInstanceRepository(db, adapter),
	}
}
