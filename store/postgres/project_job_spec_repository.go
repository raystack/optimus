package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
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
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("project_id = ? AND name = ?", repo.project.ID.UUID(), name).First(&r).Error; err != nil {
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
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("project_id = ? AND job.id in ?", repo.project.ID.UUID(), jobIDs).Find(&jobs).Error; err != nil {
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
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("project_id = ?", repo.project.ID.UUID()).Find(&jobs).Error; err != nil {
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
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("project_id = ? AND name = ?", p.ID, jobName).First(&r).Error; err != nil {
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

func (repo *ProjectJobSpecRepository) GetByDestination(ctx context.Context, destination string) (models.JobSpec, error) {
	var res Job
	if err := repo.db.WithContext(ctx).Preload("Project").Preload("Namespace").Where("destination = ?", destination).First(&res).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}

	return repo.adapter.ToSpec(res)
}

func (repo *ProjectJobSpecRepository) GetByDestinations(ctx context.Context, destinations []string) ([]models.JobSpec, error) {
	var jobs []Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("destination in ?", destinations).Find(&jobs).Error; err != nil {
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
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("project_id = ?", repo.project.ID.UUID()).Find(&jobs).Error; err != nil {
		return nil, err
	}

	namespaceToJobMapping := map[string][]string{}
	for _, job := range jobs {
		namespaceToJobMapping[job.Namespace.Name] = append(namespaceToJobMapping[job.Namespace.Name], job.Name)
	}
	repo.cache.Set(namespaceToJobMappingKey, namespaceToJobMapping, cache.DefaultExpiration)
	return cloneStringMap(namespaceToJobMapping), nil
}
