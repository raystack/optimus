package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
)

type NamespaceJobSpecRepository struct {
	db                 *gorm.DB
	namespace          models.NamespaceSpec
	projectJobSpecRepo store.ProjectJobSpecRepository
	adapter            *JobSpecAdapter
}

func (repo *NamespaceJobSpecRepository) Insert(ctx context.Context, spec models.JobSpec, jobDestination string) error {
	resource, err := repo.adapter.FromSpecWithNamespace(spec, repo.namespace, jobDestination)
	if err != nil {
		return err
	}
	if resource.Name == "" {
		return errors.New("name cannot be empty")
	}
	// if soft deleted earlier
	if err := repo.HardDelete(ctx, spec.Name); err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Create(&resource).Error
}

func (repo *NamespaceJobSpecRepository) Save(ctx context.Context, spec models.JobSpec, jobDestination string) error {
	// while saving a JobSpec, we need to ensure that it's name is unique for a project
	existingJobSpec, namespaceSpec, err := repo.projectJobSpecRepo.GetByName(ctx, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, spec, jobDestination)
	} else if err != nil {
		return fmt.Errorf("unable to retrieve spec by name: %w", err)
	}

	if namespaceSpec.ID != repo.namespace.ID {
		return fmt.Errorf("job %s already exists for the project %s", spec.Name, repo.namespace.ProjectSpec.Name)
	}

	resource, err := repo.adapter.FromJobSpec(spec, jobDestination)
	if err != nil {
		return err
	}
	resource.ID = existingJobSpec.ID
	return repo.db.WithContext(ctx).Model(&resource).Updates(&resource).Error
}

func (repo *NamespaceJobSpecRepository) GetByName(ctx context.Context, name string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("namespace_id = ? AND name = ?", repo.namespace.ID, name).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}

	return repo.adapter.ToSpec(r)
}

func (repo *NamespaceJobSpecRepository) Delete(ctx context.Context, id uuid.UUID) error {
	return repo.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(ctx).Where("id = ?", id).Delete(&Job{}).Error; err != nil {
			return err
		}
		jobSourceRepo := NewJobSourceRepository(tx)
		return jobSourceRepo.DeleteByJobID(ctx, id)
	})
}

func (repo *NamespaceJobSpecRepository) HardDelete(ctx context.Context, name string) error {
	// find the base job
	var r Job
	if err := repo.db.WithContext(ctx).Unscoped().Where("project_id = ? AND name = ?", repo.namespace.ProjectSpec.ID.UUID(), name).
		Find(&r).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		// no job exists, inserting for the first time
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to fetch soft deleted resource: %w", err)
	}
	return repo.db.WithContext(ctx).Unscoped().Where("id = ?", r.ID).Delete(&Job{}).Error
}

func (repo *NamespaceJobSpecRepository) GetAll(ctx context.Context) ([]models.JobSpec, error) {
	var specs []models.JobSpec
	var jobs []Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("namespace_id = ?", repo.namespace.ID).Find(&jobs).Error; err != nil {
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

func NewNamespaceJobSpecRepository(db *gorm.DB, namespace models.NamespaceSpec, projectJobSpecRepo store.ProjectJobSpecRepository, adapter *JobSpecAdapter) *NamespaceJobSpecRepository {
	return &NamespaceJobSpecRepository{
		db:                 db,
		namespace:          namespace,
		projectJobSpecRepo: projectJobSpecRepo,
		adapter:            adapter,
	}
}

func cloneStringMap(source map[string][]string) map[string][]string {
	mp := map[string][]string{}
	for k, v := range source {
		mp[k] = append(mp[k], v...)
	}
	return mp
}
