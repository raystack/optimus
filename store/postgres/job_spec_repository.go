package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"
)

const (
	CacheTTL     = time.Hour * 1
	CacheCleanUp = time.Minute * 15

	// [namespace name] -> []{job name,...} in a project
	namespaceToJobMappingKey = "namespaceToJobMapping"
)

type ProjectJobSpecRepository struct {
	db      *gorm.DB
	project models.ProjectSpec
	adapter *JobSpecAdapter

	mu    sync.Mutex
	cache *cache.Cache
}

func NewProjectJobSpecRepository(db *gorm.DB, project models.ProjectSpec, adapter *JobSpecAdapter) *ProjectJobSpecRepository {
	return &ProjectJobSpecRepository{
		db:      db,
		project: project,
		adapter: adapter,
		mu:      sync.Mutex{},
		cache:   cache.New(CacheTTL, CacheCleanUp),
	}
}

func (repo *ProjectJobSpecRepository) GetByName(ctx context.Context, name string) (models.JobSpec, models.NamespaceSpec, error) {
	var r Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Where("project_id = ? AND name = ?", repo.project.ID.UUID(), name).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, models.NamespaceSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, models.NamespaceSpec{}, err
	}

	jobSpec, err := repo.adapter.ToSpec(r)
	if err != nil {
		return models.JobSpec{}, models.NamespaceSpec{}, err
	}

	namespaceSpec, err := r.Namespace.ToSpec(repo.project)
	if err != nil {
		return models.JobSpec{}, models.NamespaceSpec{}, err
	}

	return jobSpec, namespaceSpec, nil
}

func (repo *ProjectJobSpecRepository) GetByIDs(ctx context.Context, jobIDs []uuid.UUID) ([]models.JobSpec, error) {
	var jobs []Job
	if err := repo.db.WithContext(ctx).Where("project_id = ? AND job.id in ?", repo.project.ID.UUID(), jobIDs).Find(&jobs).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, store.ErrResourceNotFound
		}
		return nil, err
	}

	var jobSpecs []models.JobSpec
	for _, job := range jobs {
		jobSpec, err := repo.adapter.ToSpec(job)
		if err != nil {
			return nil, err
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}

	return jobSpecs, nil
}

func (repo *ProjectJobSpecRepository) GetAll(ctx context.Context) ([]models.JobSpec, error) {
	var specs []models.JobSpec
	var jobs []Job
	if err := repo.db.WithContext(ctx).Where("project_id = ?", repo.project.ID.UUID()).Find(&jobs).Error; err != nil {
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

func (repo *ProjectJobSpecRepository) GetByNameForProject(ctx context.Context, projName, jobName string) (models.JobSpec, models.ProjectSpec, error) {
	var r Job
	var p Project
	if err := repo.db.WithContext(ctx).Where("name = ?", projName).First(&p).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, models.ProjectSpec{}, fmt.Errorf("project not found: %w", store.ErrResourceNotFound)
		}
		return models.JobSpec{}, models.ProjectSpec{}, err
	}
	if err := repo.db.WithContext(ctx).Where("project_id = ? AND name = ?", p.ID, jobName).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, models.ProjectSpec{}, fmt.Errorf("job spec not found: %w", store.ErrResourceNotFound)
		}
		return models.JobSpec{}, models.ProjectSpec{}, err
	}

	jSpec, err := repo.adapter.ToSpec(r)
	if err != nil {
		return models.JobSpec{}, models.ProjectSpec{}, err
	}

	pSpec := p.ToSpec()

	return jSpec, pSpec, err
}

func (repo *ProjectJobSpecRepository) GetByDestination(ctx context.Context, destination string) ([]store.ProjectJobPair, error) {
	var res []Job
	if err := repo.db.WithContext(ctx).Preload("Project").Where("destination = ?", destination).Find(&res).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, store.ErrResourceNotFound
		}
		return nil, err
	}

	var pairs []store.ProjectJobPair
	for _, job := range res {
		jSpec, err := repo.adapter.ToSpec(job)
		if err != nil {
			return nil, err
		}
		pSpec := job.Project.ToSpec()

		pairs = append(pairs, store.ProjectJobPair{
			Project: pSpec,
			Job:     jSpec,
		})
	}
	return pairs, nil
}

func (repo *ProjectJobSpecRepository) GetJobNamespaces(ctx context.Context) (map[string][]string, error) {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if raw, ok := repo.cache.Get(namespaceToJobMappingKey); ok {
		mapping := raw.(map[string][]string)
		if len(mapping) != 0 {
			return cloneStringMap(mapping), nil
		}
	}

	var jobs []Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Where("project_id = ?", repo.project.ID.UUID()).Find(&jobs).Error; err != nil {
		return nil, err
	}

	namespaceToJobMapping := map[string][]string{}
	for _, job := range jobs {
		namespaceToJobMapping[job.Namespace.Name] = append(namespaceToJobMapping[job.Namespace.Name], job.Name)
	}
	repo.cache.Set(namespaceToJobMappingKey, namespaceToJobMapping, cache.DefaultExpiration)
	return cloneStringMap(namespaceToJobMapping), nil
}

type JobSpecRepository struct {
	db                 *gorm.DB
	namespace          models.NamespaceSpec
	projectJobSpecRepo store.ProjectJobSpecRepository
	adapter            *JobSpecAdapter
}

func (repo *JobSpecRepository) Insert(ctx context.Context, spec models.JobSpec) error {
	resource, err := repo.adapter.FromSpecWithNamespace(spec, repo.namespace)
	if err != nil {
		return err
	}
	if len(resource.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	// if soft deleted earlier
	if err := repo.HardDelete(ctx, spec.Name); err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Create(&resource).Error
}

func (repo *JobSpecRepository) Save(ctx context.Context, spec models.JobSpec) error {
	// while saving a JobSpec, we need to ensure that it's name is unique for a project
	existingJobSpec, namespaceSpec, err := repo.projectJobSpecRepo.GetByName(ctx, spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(ctx, spec)
	} else if err != nil {
		return fmt.Errorf("unable to retrieve spec by name: %w", err)
	}

	if namespaceSpec.ID != repo.namespace.ID {
		return fmt.Errorf("job %s already exists for the project %s", spec.Name, repo.namespace.ProjectSpec.Name)
	}

	resource, err := repo.adapter.FromJobSpec(spec)
	if err != nil {
		return err
	}
	resource.ID = existingJobSpec.ID
	return repo.db.WithContext(ctx).Model(&resource).Updates(&resource).Error
}

func (repo *JobSpecRepository) GetByID(ctx context.Context, id uuid.UUID) (models.JobSpec, error) {
	var r Job
	if err := repo.db.WithContext(ctx).Where("namespace_id = ? AND id = ?", repo.namespace.ID, id).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}

	return repo.adapter.ToSpec(r)
}

func (repo *JobSpecRepository) GetByName(ctx context.Context, name string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.WithContext(ctx).Where("namespace_id = ? AND name = ?", repo.namespace.ID, name).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}

	return repo.adapter.ToSpec(r)
}

func (repo *JobSpecRepository) Delete(ctx context.Context, name string) error {
	return repo.db.WithContext(ctx).Where("namespace_id = ? AND name = ?", repo.namespace.ID, name).Delete(&Job{}).Error
}

func (repo *JobSpecRepository) HardDelete(ctx context.Context, name string) error {
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

func (repo *JobSpecRepository) GetAll(ctx context.Context) ([]models.JobSpec, error) {
	var specs []models.JobSpec
	var jobs []Job
	if err := repo.db.WithContext(ctx).Where("namespace_id = ?", repo.namespace.ID).Find(&jobs).Error; err != nil {
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

func NewJobSpecRepository(db *gorm.DB, namespace models.NamespaceSpec, projectJobSpecRepo store.ProjectJobSpecRepository, adapter *JobSpecAdapter) *JobSpecRepository {
	return &JobSpecRepository{
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
