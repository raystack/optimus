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

type jobSpecRepository struct {
	db *gorm.DB

	adapter *JobSpecAdapter
}

// NewJobSpecRepository initializes job spec repository
func NewJobSpecRepository(db *gorm.DB, adapter *JobSpecAdapter) (store.JobSpecRepository, error) {
	if db == nil {
		return nil, errors.New("db client is nil")
	}
	if adapter == nil {
		return nil, errors.New("adapter is nil")
	}
	return &jobSpecRepository{
		db:      db,
		adapter: adapter,
	}, nil
}

func (j jobSpecRepository) GetAllByProjectName(ctx context.Context, projectName string, includeDeleted bool) ([]models.JobSpec, error) {
	query := j.db.WithContext(ctx).
		Preload("Namespace").
		Preload("Project").
		Joins("Project").
		Where(`"Project"."name" = ?`, projectName)

	if includeDeleted {
		query = query.Unscoped()
	}

	var jobs []Job
	if err := query.Find(&jobs).Error; err != nil {
		return nil, err
	}
	return j.toJobSpecs(jobs)
}

func (j jobSpecRepository) GetAllByProjectNameAndNamespaceName(ctx context.Context, projectName, namespaceName string, includeDeleted bool) ([]models.JobSpec, error) {
	query := j.db.WithContext(ctx).
		Preload("Namespace").
		Preload("Project").
		Joins("Project").
		Joins("Namespace").
		Where(`"Project"."name" = ? and "Namespace"."name" = ?`, projectName, namespaceName)

	if includeDeleted {
		query = query.Unscoped()
	}

	var jobs []Job
	if err := query.Find(&jobs).Error; err != nil {
		return nil, err
	}
	return j.toJobSpecs(jobs)
}

func (j jobSpecRepository) GetByNameAndProjectName(ctx context.Context, name, projectName string, includeDeleted bool) (models.JobSpec, error) {
	job, err := j.getByNameAndProjectName(ctx, name, projectName, includeDeleted)
	if err != nil {
		return models.JobSpec{}, err
	}
	return j.adapter.ToSpec(job)
}

func (j jobSpecRepository) GetByResourceDestinationURN(ctx context.Context, resourceDestinationURN string, includeDeleted bool) ([]models.JobSpec, error) {
	query := j.db.WithContext(ctx).
		Preload("Namespace").
		Preload("Project").
		Where("destination = ?", resourceDestinationURN)

	if includeDeleted {
		query = query.Unscoped()
	}

	var jobs []Job
	if err := query.Find(&jobs).Error; err != nil {
		return []models.JobSpec{}, err
	}
	if len(jobs) == 0 {
		return []models.JobSpec{}, store.ErrResourceNotFound
	}
	return j.toJobSpecs(jobs)
}

func (j jobSpecRepository) GetDependentJobs(ctx context.Context, jobName, resourceDestinationURN, projectName string) ([]models.JobSpec, error) {
	var allDependentJobs []Job
	dependentJobsInferred, err := j.getDependentJobsInferred(ctx, resourceDestinationURN)
	if err != nil {
		return nil, err
	}
	allDependentJobs = append(allDependentJobs, dependentJobsInferred...)

	dependentJobsStatic, err := j.getDependentJobsStatic(ctx, jobName, projectName)
	if err != nil {
		return nil, err
	}
	allDependentJobs = append(allDependentJobs, dependentJobsStatic...)

	return j.toJobSpecs(allDependentJobs)
}

func (j jobSpecRepository) GetInferredDependenciesPerJobID(ctx context.Context, projectName string) (map[uuid.UUID][]models.JobSpec, error) {
	var jobDependencies []jobDependency
	if err := j.db.WithContext(ctx).
		Preload("DependencyNamespace").Preload("DependencyProject").
		Select("js.job_id, "+
			"j.id as dependency_id, "+
			"j.name as dependency_name, "+
			"j.task_name as dependency_task_name, "+
			"j.destination as dependency_destination, "+
			"j.namespace_id as dependency_namespace_id, "+
			"j.project_id as dependency_project_id").
		Joins("join job j on js.resource_urn = j.destination").
		Joins("join project p on js.project_id = p.id").
		Table("job_source js").
		Where("p.name = ? and j.deleted_at is null", projectName).
		Find(&jobDependencies).Error; err != nil {
		return nil, err
	}
	return j.adapter.groupToDependenciesPerJobID(jobDependencies)
}

func (j jobSpecRepository) GetStaticDependenciesPerJobID(ctx context.Context, projectName string) (map[uuid.UUID][]models.JobSpec, error) {
	var jobDependencies []jobDependency
	requestedJobsQuery := j.db.
		Select("j.id, j.name, jsonb_object_keys(j.dependencies) as dependency_name, j.project_id").
		Table("job j").
		Joins("join project p on j.project_id = p.id").
		Where("p.name = ?", projectName)
	dependenciesQuery := j.db.
		Select("j.id, j.name, j.namespace_id, j.task_name, j.project_id, p.name as project_name").
		Table("job j").Joins("join project p on j.project_id = p.id")

	if err := j.db.WithContext(ctx).
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
			"(rj.dependency_name=d.name and rj.project_id=d.project_id) or "+
			"rj.dependency_name=d.project_name || '/' ||d.name)", dependenciesQuery).
		Find(&jobDependencies).Error; err != nil {
		return nil, err
	}
	return j.adapter.groupToDependenciesPerJobID(jobDependencies)
}

func (j jobSpecRepository) Save(ctx context.Context, incomingJobSpec models.JobSpec) error {
	incomingJob, err := j.adapter.FromJobSpec(incomingJobSpec, incomingJobSpec.ResourceDestination)
	if err != nil {
		return err
	}

	existingJob, err := j.getByNameAndProjectName(ctx, incomingJob.Name, incomingJob.Project.Name, false)
	if errors.Is(err, store.ErrResourceNotFound) {
		return j.insert(ctx, incomingJob)
	} else if err != nil {
		return fmt.Errorf("unable to retrieve spec by name: %w", err)
	}

	zeroUUID := uuid.UUID{}
	if incomingJob.NamespaceID != zeroUUID && incomingJob.NamespaceID != existingJob.NamespaceID {
		return fmt.Errorf("job [%s] already exists in namespace [%s] for project [%s]", incomingJob.Name, existingJob.Namespace.Name, existingJob.Project.Name)
	}

	incomingJob.ID = existingJob.ID
	return j.db.WithContext(ctx).Model(&incomingJob).Updates(&incomingJob).Error
}

func (j jobSpecRepository) DeleteByID(ctx context.Context, id uuid.UUID) error {
	return j.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(ctx).Where("id = ?", id).Delete(&Job{}).Error; err != nil {
			return err
		}
		return tx.WithContext(ctx).Where("job_id = ?", id).Delete(&JobSource{}).Error
	})
}

func (j jobSpecRepository) getByNameAndProjectName(ctx context.Context, name, projectName string, includeDeleted bool) (Job, error) {
	query := j.db.WithContext(ctx).
		Preload("Namespace").
		Preload("Project").
		Joins("Project").
		Where(`"job"."name" = ? and "Project"."name" = ?`, name, projectName)

	if includeDeleted {
		query = query.Unscoped()
	}

	var job Job
	if err := query.First(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return Job{}, store.ErrResourceNotFound
		}
		return Job{}, err
	}
	return job, nil
}

func (j jobSpecRepository) insert(ctx context.Context, job Job) error {
	if err := j.hardDelete(ctx, job.Name, job.Project.Name); err != nil {
		return err
	}
	return j.db.WithContext(ctx).Create(&job).Error
}

func (j jobSpecRepository) hardDelete(ctx context.Context, name, projectName string) error {
	var job Job
	if err := j.db.WithContext(ctx).
		Unscoped().
		Joins("Project").
		Where(`"job"."name" = ? and "Project"."name" = ?`, name, projectName).
		First(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return fmt.Errorf("failed to fetch soft deleted resource: %w", err)
	}
	return j.db.WithContext(ctx).Unscoped().Where("id = ?", job.ID).Delete(&Job{}).Error
}

func (j jobSpecRepository) getDependentJobsInferred(ctx context.Context, resourceDestinationURN string) ([]Job, error) {
	var jobs []Job
	subQuery := j.db.Select("job_id").
		Where("resource_urn = ?", resourceDestinationURN).
		Table("job_source")
	if err := j.db.WithContext(ctx).
		Preload("Namespace").
		Preload("Project").
		Where("id IN (?)", subQuery).
		Find(&jobs).Error; err != nil {
		return nil, err
	}
	return jobs, nil
}

func (j jobSpecRepository) getDependentJobsStatic(ctx context.Context, name, projectName string) ([]Job, error) {
	var jobs []Job
	projectAndJobName := fmt.Sprintf("%s/%s", projectName, name)
	if err := j.db.WithContext(ctx).
		Preload("Namespace").
		Preload("Project").
		Joins("Project").
		Where(`(("job"."dependencies" -> ?) IS NOT NULL and "Project"."name" = ?) or ("job".dependencies -> ?) IS NOT NULL`, name, projectName, projectAndJobName).
		Find(&jobs).Error; err != nil {
		return nil, err
	}
	return jobs, nil
}

func (j jobSpecRepository) toJobSpecs(jobs []Job) ([]models.JobSpec, error) {
	jobSpecs := make([]models.JobSpec, len(jobs))
	for i, job := range jobs {
		jobSpec, err := j.adapter.ToSpec(job)
		if err != nil {
			return nil, err
		}
		jobSpecs[i] = jobSpec
	}
	return jobSpecs, nil
}
