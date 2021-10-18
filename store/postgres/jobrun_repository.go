package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type JobRunRepository struct {
	db           *gorm.DB
	adapter      *JobSpecAdapter
	instanceRepo *InstanceRepository
}

func (repo *JobRunRepository) Insert(ctx context.Context, namespace models.NamespaceSpec, spec models.JobRun) error {
	resource, err := repo.adapter.FromJobRun(spec, namespace)
	if err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&resource).Error
}

func (repo *JobRunRepository) Save(ctx context.Context, namespace models.NamespaceSpec, spec models.JobRun) error {
	if spec.Status == "" {
		// mark default state pending
		spec.Status = models.RunStatePending
	}

	existingResource, _, err := repo.GetByID(ctx, spec.ID)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, namespace, spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to find jobrun by id")
	}

	resource, err := repo.adapter.FromJobRun(spec, namespace)
	if err != nil {
		return err
	}
	resource.ID = existingResource.ID
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Model(&resource).Updates(&resource).Error
}

func (repo *JobRunRepository) GetByID(ctx context.Context, id uuid.UUID) (models.JobRun, models.NamespaceSpec, error) {
	var r JobRun
	if err := repo.db.WithContext(ctx).Preload("Namespace").Where("id = ?", id).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.JobRun{}, models.NamespaceSpec{}, err
	}
	if instances, err := repo.instanceRepo.GetByJobRun(ctx, r.ID); err == nil {
		r.Instances = instances
	}
	return repo.adapter.ToJobRun(r)
}

func (repo *JobRunRepository) GetByScheduledAt(ctx context.Context, jobID uuid.UUID, scheduledAt time.Time) (models.JobRun, models.NamespaceSpec, error) {
	var r JobRun
	if err := repo.db.WithContext(ctx).Preload("Namespace").Where("job_id = ? AND scheduled_at = ?", jobID, scheduledAt).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRun{}, models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.JobRun{}, models.NamespaceSpec{}, err
	}
	if instances, err := repo.instanceRepo.GetByJobRun(ctx, r.ID); err == nil {
		r.Instances = instances
	}
	return repo.adapter.ToJobRun(r)
}

// AddInstance associate instance details
func (repo *JobRunRepository) AddInstance(ctx context.Context, namespaceSpec models.NamespaceSpec, run models.JobRun, spec models.InstanceSpec) error {
	instance, err := repo.instanceRepo.GetByName(ctx, run.ID, spec.Name, spec.Type.String())
	if err != nil && !errors.Is(err, store.ErrResourceNotFound) {
		return err
	}
	if instance.ID.String() != "" {
		// delete if associated before
		if err := repo.instanceRepo.Delete(ctx, instance.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	return repo.instanceRepo.Save(ctx, run, spec)
}

// ClearInstance deletes associated instance details
func (repo *JobRunRepository) ClearInstance(ctx context.Context, runID uuid.UUID, instanceType models.InstanceType, instanceName string) error {
	r, _, err := repo.GetByID(ctx, runID)
	if err != nil {
		return err
	}
	for _, instance := range r.Instances {
		if instance.Name == instanceName && instance.Type == instanceType {
			if err := repo.instanceRepo.Delete(ctx, instance.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			break
		}
	}
	return nil
}

// Clear prepares job run for fresh start
func (repo *JobRunRepository) Clear(ctx context.Context, runID uuid.UUID) error {
	if err := repo.instanceRepo.DeleteByJobRun(ctx, runID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return repo.db.WithContext(ctx).Model(&JobRun{ID: runID}).Updates(JobRun{Status: models.RunStatePending.String()}).Error
}

func (repo *JobRunRepository) Delete(ctx context.Context, id uuid.UUID) error {
	if err := repo.instanceRepo.DeleteByJobRun(ctx, id); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return repo.db.WithContext(ctx).Where("id = ?", id).Delete(&JobRun{}).Error
}

func (repo *JobRunRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status models.JobRunState) error {
	var jr JobRun
	if err := repo.db.Where("id = ?", id).Find(&jr).Error; err != nil {
		return err
	}
	jr.Status = status.String()
	return repo.db.Omit("Namespace").Save(jr).Error
}

func (repo *JobRunRepository) GetByStatus(ctx context.Context, statuses ...models.JobRunState) ([]models.JobRun, error) {
	var specs []models.JobRun
	var runs []JobRun
	if err := repo.db.WithContext(ctx).Where("status IN (?)", statuses).Find(&runs).Error; err != nil {
		return specs, err
	}

	for _, run := range runs {
		if instances, err := repo.instanceRepo.GetByJobRun(ctx, run.ID); err == nil {
			run.Instances = instances
		}
		adapt, _, err := repo.adapter.ToJobRun(run)
		if err != nil {
			return specs, err
		}
		specs = append(specs, adapt)
	}
	return specs, nil
}

func (repo *JobRunRepository) GetByTrigger(ctx context.Context, trigger models.JobRunTrigger, statuses ...models.JobRunState) ([]models.JobRun, error) {
	var specs []models.JobRun
	var runs []JobRun
	if len(statuses) > 0 {
		if err := repo.db.WithContext(ctx).Where("trigger = ? and status IN (?)", trigger, statuses).Find(&runs).Error; err != nil {
			return specs, err
		}
	} else {
		if err := repo.db.WithContext(ctx).Where("trigger = ?", trigger).Find(&runs).Error; err != nil {
			return specs, err
		}
	}

	for _, run := range runs {
		if instances, err := repo.instanceRepo.GetByJobRun(ctx, run.ID); err == nil {
			run.Instances = instances
		}
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
