package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"gorm.io/gorm"
)

type JobDependency struct {
	JobID     uuid.UUID `gorm:"not null" json:"job_id"`
	ProjectID uuid.UUID `gorm:"not null" json:"project_id"`
	Project   Project   `gorm:"foreignKey:ProjectID"`

	DependentJobID     uuid.UUID `gorm:"not null" json:"dependent_job_id"`
	DependentProjectID uuid.UUID `gorm:"not null" json:"dependent_project_id"`
	DependentProject   Project   `gorm:"foreignKey:DependentProjectID"`

	Type string

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
}

type jobDependencyRepository struct {
	db *gorm.DB
}

type JobDependencies []JobDependency

func (d JobDependencies) ToSpec() ([]models.JobIDDependenciesPair, error) {
	var jobDependencies []models.JobIDDependenciesPair
	for _, dependency := range d {
		dependentProject := dependency.DependentProject.ToSpec()
		jobDependencies = append(jobDependencies, models.JobIDDependenciesPair{
			JobID:            dependency.JobID,
			DependentProject: dependentProject,
			DependentJobID:   dependency.DependentJobID,
			Type:             models.JobSpecDependencyType(dependency.Type),
		})
	}
	return jobDependencies, nil
}

func (d JobDependency) FromSpec(projectID models.ProjectID, jobID uuid.UUID, jobDependency models.JobSpecDependency) JobDependency {
	return JobDependency{
		JobID:              jobID,
		ProjectID:          projectID.UUID(),
		DependentJobID:     jobDependency.Job.ID,
		DependentProjectID: jobDependency.Project.ID.UUID(),
		Type:               jobDependency.Type.String(),
	}
}

func (repo *jobDependencyRepository) Save(ctx context.Context, projectID models.ProjectID, jobID uuid.UUID, jobDependencySpec models.JobSpecDependency) error {
	jobDependency := JobDependency{}.FromSpec(projectID, jobID, jobDependencySpec)
	return repo.db.WithContext(ctx).Create(&jobDependency).Error
}

func (repo *jobDependencyRepository) GetAll(ctx context.Context, projectID models.ProjectID) ([]models.JobIDDependenciesPair, error) {
	var jobDependencies []JobDependency
	if err := repo.db.WithContext(ctx).Preload("Project").Where("project_id = ?", projectID.UUID()).Find(&jobDependencies).Error; err != nil {
		return nil, err
	}

	return JobDependencies(jobDependencies).ToSpec()
}

func (repo *jobDependencyRepository) DeleteByJobID(ctx context.Context, jobID uuid.UUID) error {
	return repo.db.WithContext(ctx).Where("job_id = ?", jobID).Delete(&JobDependency{}).Error
}

func NewJobDependencyRepository(db *gorm.DB) *jobDependencyRepository {
	return &jobDependencyRepository{
		db: db,
	}
}
