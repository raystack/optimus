package postgres

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
)

type projectJobSpecRepository struct {
	db      *gorm.DB
	project models.ProjectSpec
	adapter *JobSpecAdapter
}

func NewProjectJobRepository(db *gorm.DB, project models.ProjectSpec, adapter *JobSpecAdapter) *projectJobSpecRepository {
	return &projectJobSpecRepository{
		db:      db,
		project: project,
		adapter: adapter,
	}
}

func (repo *projectJobSpecRepository) GetByName(name string) (models.JobSpec, models.NamespaceSpec, error) {
	var r Job
	if err := repo.db.Preload("Namespace").Where("project_id = ? AND name = ?", repo.project.ID, name).Find(&r).Error; err != nil {
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

func (repo *projectJobSpecRepository) GetAll() ([]models.JobSpec, error) {
	specs := []models.JobSpec{}
	jobs := []Job{}
	if err := repo.db.Where("project_id = ?", repo.project.ID).Find(&jobs).Error; err != nil {
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

func (repo *projectJobSpecRepository) GetByDestination(destination string) (models.JobSpec, models.ProjectSpec, error) {
	var r Job
	if err := repo.db.Preload("Project").Where("destination = ?", destination).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, models.ProjectSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, models.ProjectSpec{}, err
	}

	jSpec, err := repo.adapter.ToSpec(r)
	if err != nil {
		return models.JobSpec{}, models.ProjectSpec{}, err
	}

	pSpec, err := r.Project.ToSpec()
	if err != nil {
		return models.JobSpec{}, models.ProjectSpec{}, err
	}

	return jSpec, pSpec, err
}

type jobSpecRepository struct {
	db                 *gorm.DB
	namespace          models.NamespaceSpec
	projectJobSpecRepo store.ProjectJobSpecRepository
	adapter            *JobSpecAdapter
}

func (repo *jobSpecRepository) Insert(spec models.JobSpec) error {
	resource, err := repo.adapter.FromSpecWithNamespace(spec, repo.namespace)
	if err != nil {
		return err
	}
	if len(resource.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	// if soft deleted earlier
	if err := repo.HardDelete(spec.Name); err != nil {
		return err
	}
	return repo.db.Create(&resource).Error
}

func (repo *jobSpecRepository) Save(spec models.JobSpec) error {
	// while saving a JobSpec, we need to ensure that it's name is unique for a project
	existingJobSpec, namespaceSpec, err := repo.projectJobSpecRepo.GetByName(spec.Name)
	if errors.Is(err, store.ErrResourceNotFound) {
		return repo.Insert(spec)
	} else if err != nil {
		return errors.Wrap(err, "unable to retrieve spec by name")
	}

	if namespaceSpec.ID != repo.namespace.ID {
		return errors.New(fmt.Sprintf("job %s already exists for the project %s", spec.Name, repo.namespace.ProjectSpec.Name))
	}

	resource, err := repo.adapter.FromSpec(spec)
	if err != nil {
		return err
	}
	resource.ID = existingJobSpec.ID

	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *jobSpecRepository) GetByID(id uuid.UUID) (models.JobSpec, error) {
	var r Job
	if err := repo.db.Where("namespace_id = ? AND id = ?", repo.namespace.ID, id).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}

	return repo.adapter.ToSpec(r)
}

func (repo *jobSpecRepository) GetByName(name string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.Where("namespace_id = ? AND name = ?", repo.namespace.ID, name).Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobSpec{}, store.ErrResourceNotFound
		}
		return models.JobSpec{}, err
	}

	return repo.adapter.ToSpec(r)
}

func (repo *jobSpecRepository) Delete(name string) error {
	return repo.db.Where("namespace_id = ? AND name = ?", repo.namespace.ID, name).Delete(&Job{}).Error
}

func (repo *jobSpecRepository) HardDelete(name string) error {
	//find the base job
	var r Job
	if err := repo.db.Unscoped().Where("project_id = ? AND name = ?", repo.namespace.ProjectSpec.ID, name).Find(&r).Error; err == gorm.ErrRecordNotFound {
		// no job exists, inserting for the first time
		return nil
	} else if err != nil {
		return errors.Wrap(err, "failed to fetch soft deleted resource")
	}
	// cascade delete instances
	if err := repo.db.Unscoped().Where("job_id = ?", r.ID).Delete(&Instance{}).Error; err != nil {
		return errors.Wrap(err, "failed to cascade delete instances for the job")
	}
	return repo.db.Unscoped().Where("id = ?", r.ID).Delete(&Job{}).Error
}

func (repo *jobSpecRepository) GetAll() ([]models.JobSpec, error) {
	specs := []models.JobSpec{}
	jobs := []Job{}
	if err := repo.db.Where("namespace_id = ?", repo.namespace.ID).Find(&jobs).Error; err != nil {
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

func NewJobRepository(db *gorm.DB, namespace models.NamespaceSpec, projectJobSpecRepo store.ProjectJobSpecRepository, adapter *JobSpecAdapter) *jobSpecRepository {
	return &jobSpecRepository{
		db:                 db,
		namespace:          namespace,
		projectJobSpecRepo: projectJobSpecRepo,
		adapter:            adapter,
	}
}
