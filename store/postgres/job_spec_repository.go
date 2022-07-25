package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type jobSpecRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
}

func NewJobSpecRepository(db *gorm.DB, adapter *JobSpecAdapter) store.JobSpecRepository {
	return &jobSpecRepository{
		db:      db,
		adapter: adapter,
	}
}

func (repo jobSpecRepository) GetAllByProjectID(ctx context.Context, projectID models.ProjectID) ([]models.JobSpec, error) {
	var jobs []Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("project_id = ?", projectID.UUID()).Find(&jobs).Error; err != nil {
		return nil, err
	}

	jobSpecs := make([]models.JobSpec, len(jobs))
	for i, job := range jobs {
		adapt, err := repo.adapter.ToSpec(job)
		if err != nil {
			return nil, err
		}
		jobSpecs[i] = adapt
	}
	return jobSpecs, nil
}

func (repo jobSpecRepository) GetJobByName(ctx context.Context, jobName string) ([]models.JobSpec, error) {
	var jobs []Job
	var specs []models.JobSpec
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("name = ?", jobName).Find(&jobs).Error; err != nil {
		return nil, err
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

func (repo jobSpecRepository) GetJobByResourceDestination(ctx context.Context, resourceDestination string) (models.JobSpec, error) {
	var job Job
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("destination = ?", resourceDestination).First(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
	}
	return repo.adapter.ToSpec(job)
}

func (repo jobSpecRepository) GetDependentJobs(ctx context.Context, jobSpec *models.JobSpec) ([]models.JobSpec, error) {
	var allDependentJobs []models.JobSpec
	dependentJobsInferred, err := repo.getDependentJobsInferred(ctx, jobSpec)
	if err != nil {
		return nil, err
	}
	allDependentJobs = append(allDependentJobs, dependentJobsInferred...)

	dependentJobsStatic, err := repo.getDependentJobsStatic(ctx, jobSpec)
	if err != nil {
		return nil, err
	}
	allDependentJobs = append(allDependentJobs, dependentJobsStatic...)

	return allDependentJobs, nil
}

func (repo jobSpecRepository) GetInferredDependenciesPerJobID(ctx context.Context, projectID models.ProjectID) (map[uuid.UUID][]models.JobSpec, error) {
	var jobDependencies []jobDependency
	if err := repo.db.WithContext(ctx).
		Preload("DependencyNamespace").Preload("DependencyProject").
		Select("js.job_id, "+
			"j.id as dependency_id, "+
			"j.name as dependency_name, "+
			"j.task_name as dependency_task_name, "+
			"j.destination as dependency_destination, "+
			"j.namespace_id as dependency_namespace_id, "+
			"j.project_id as dependency_project_id").
		Joins("join job j on js.resource_urn = j.destination").
		Table("job_source js").
		Where("js.project_id = ?", projectID.UUID()).
		Find(&jobDependencies).Error; err != nil {
		return nil, err
	}
	return repo.adapter.groupToDependenciesPerJobID(jobDependencies)
}

func (repo jobSpecRepository) GetStaticDependenciesPerJobID(ctx context.Context, projectID models.ProjectID) (map[uuid.UUID][]models.JobSpec, error) {
	var jobDependencies []jobDependency
	requestedJobsQuery := repo.db.
		Select("id, name, jsonb_object_keys(dependencies) as dependency_name").
		Table("job").Where("project_id = ?", projectID.UUID())
	dependenciesQuery := repo.db.
		Select("j.id, j.name, j.namespace_id, j.task_name, j.project_id, p.name as project_name").
		Table("job j").Joins("join project p on j.project_id = p.id")

	if err := repo.db.WithContext(ctx).
		Preload("DependencyNamespace").Preload("DependencyProject").
		Select("rj.id as job_id, "+
			"rj.name as job_name, "+
			"d.id as dependency_id, "+
			"d.name as dependency_name, "+
			"d.task_name as dependency_task_name, "+
			"d.namespace_id as dependency_namespace_id, "+
			"d.project_id as dependency_project_id").
		Table("(?) rj", requestedJobsQuery).
		Joins("join (?) d on ("+
			"rj.dependency_name=d.name or "+
			"rj.dependency_name=d.project_name || '/' ||d.name)", dependenciesQuery).
		Find(&jobDependencies).Error; err != nil {
		return nil, err
	}
	return repo.adapter.groupToDependenciesPerJobID(jobDependencies)
}

func (repo jobSpecRepository) getDependentJobsInferred(ctx context.Context, jobSpec *models.JobSpec) ([]models.JobSpec, error) {
	var jobs []Job
	var specs []models.JobSpec

	subQuery := repo.db.Select("job_id").Where("resource_urn = ?", jobSpec.ResourceDestination).Table("job_source")
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").Where("id IN (?)", subQuery).Find(&jobs).Error; err != nil {
		return nil, err
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

func (repo jobSpecRepository) getDependentJobsStatic(ctx context.Context, jobSpec *models.JobSpec) ([]models.JobSpec, error) {
	var jobs []Job
	var specs []models.JobSpec

	projectAndJobName := fmt.Sprintf("%s/%s", jobSpec.GetProjectSpec().Name, jobSpec.Name)
	if err := repo.db.WithContext(ctx).Preload("Namespace").Preload("Project").
		Where("((dependencies -> ?) IS NOT NULL or (dependencies -> ?) IS NOT NULL)", jobSpec.Name, projectAndJobName).Find(&jobs).Error; err != nil {
		return nil, err
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
