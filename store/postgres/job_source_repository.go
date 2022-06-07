package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type JobSource struct {
	JobID     uuid.UUID `gorm:"not null" json:"job_id"`
	ProjectID uuid.UUID `gorm:"not null" json:"project_id"`

	ResourceURN string `gorm:"not null" json:"resource_urn"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
}

type jobSourceRepository struct {
	db *gorm.DB
}

type JobSources []JobSource

func (d JobSources) ToSpec() ([]models.JobSource, error) {
	var jobSources []models.JobSource
	for _, source := range d {
		jobSources = append(jobSources, models.JobSource{
			JobID:       source.JobID,
			ProjectID:   models.ProjectID(source.ProjectID),
			ResourceURN: source.ResourceURN,
		})
	}
	return jobSources, nil
}

func (JobSource) FromSpec(jobSource models.JobSource) JobSource {
	return JobSource{
		JobID:       jobSource.JobID,
		ProjectID:   jobSource.ProjectID.UUID(),
		ResourceURN: jobSource.ResourceURN,
	}
}

func (repo *jobSourceRepository) Save(ctx context.Context, jobSourceSpec models.JobSource) error {
	jobSource := JobSource{}.FromSpec(jobSourceSpec)
	return repo.db.WithContext(ctx).Create(&jobSource).Error
}

func (repo *jobSourceRepository) GetAll(ctx context.Context, projectID models.ProjectID) ([]models.JobSource, error) {
	var jobSources []JobSource
	if err := repo.db.WithContext(ctx).Where("project_id = ?", projectID.UUID()).Find(&jobSources).Error; err != nil {
		return nil, err
	}

	return JobSources(jobSources).ToSpec()
}

func (repo *jobSourceRepository) DeleteByJobID(ctx context.Context, jobID uuid.UUID) error {
	return repo.db.WithContext(ctx).Where("job_id = ?", jobID).Delete(&JobSource{}).Error
}

func NewJobSourceRepository(db *gorm.DB) *jobSourceRepository {
	return &jobSourceRepository{
		db: db,
	}
}
