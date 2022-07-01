package postgres

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type jobSpecRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
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
		return models.JobSpec{}, err
	}
	jobSpec, err := repo.adapter.ToSpec(job)
	if err != nil {
		return models.JobSpec{}, err
	}
	return jobSpec, nil
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

func NewJobSpecRepository(db *gorm.DB, adapter *JobSpecAdapter) *jobSpecRepository {
	return &jobSpecRepository{
		db:      db,
		adapter: adapter,
	}
}
