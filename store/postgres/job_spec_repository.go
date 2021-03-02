package postgres

import (
	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

type jobSpecRepository struct {
	db      *gorm.DB
	project models.ProjectSpec
	adapter *Adapter
}

func (repo *jobSpecRepository) Insert(spec models.JobSpec) error {
	resource, err := repo.adapter.FromSpecWithProject(spec, repo.project)
	if err != nil {
		return err
	}
	if len(resource.Name) == 0 {
		return errors.New("name cannot be empty")
	}
	return repo.db.Create(&resource).Error
}

func (repo *jobSpecRepository) Save(spec models.JobSpec) error {
	existingResource, err := repo.GetByName(spec.Name)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return repo.Insert(spec)
	}
	resource, err := repo.adapter.FromSpec(spec)
	if err != nil {
		return err
	}
	resource.ID = existingResource.ID
	return repo.db.Model(resource).Updates(resource).Error
}

func (repo *jobSpecRepository) GetByID(id uuid.UUID) (models.JobSpec, error) {
	var r Job
	if err := repo.db.Preload("Project").Where("id = ?", id).Find(&r).Error; err != nil {
		return models.JobSpec{}, err
	}
	return repo.adapter.ToSpec(r)
}

func (repo *jobSpecRepository) GetByName(name string) (models.JobSpec, error) {
	var r Job
	if err := repo.db.Preload("Project").Where("project_id = ? AND name = ?", repo.project.ID, name).Find(&r).Error; err != nil {
		return models.JobSpec{}, err
	}
	return repo.adapter.ToSpec(r)
}

func (repo *jobSpecRepository) Delete(name string) error {
	return repo.db.Where("project_id = ? AND name = ?", repo.project.ID, name).Delete(&Job{}).Error
}

func (repo *jobSpecRepository) GetAll() ([]models.JobSpec, error) {
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

func NewJobRepository(db *gorm.DB, project models.ProjectSpec, adapter *Adapter) *jobSpecRepository {
	return &jobSpecRepository{
		db:      db,
		project: project,
		adapter: adapter,
	}
}
