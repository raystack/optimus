package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store"
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

type jobSources []JobSource

func (j jobSources) ToSpec() []models.JobSource {
	var output []models.JobSource
	for _, source := range j {
		output = append(output, models.JobSource{
			JobID:       source.JobID,
			ProjectID:   models.ProjectID(source.ProjectID),
			ResourceURN: source.ResourceURN,
		})
	}
	return output
}

func (repo *jobSourceRepository) Save(ctx context.Context, projectID models.ProjectID, jobID uuid.UUID, jobSourceURNs []string) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := repo.delete(ctx, tx, jobID); err != nil {
			return err
		}
		for _, urn := range jobSourceURNs {
			jobSource := &JobSource{
				JobID:       jobID,
				ProjectID:   projectID.UUID(),
				ResourceURN: urn,
			}
			if response := tx.WithContext(ctx).Create(jobSource); response.Error != nil {
				return response.Error
			}
		}
		return nil
	})
}

func (repo *jobSourceRepository) GetAll(ctx context.Context, projectID models.ProjectID) ([]models.JobSource, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	var sources []JobSource
	if err := repo.db.WithContext(ctx).Where("project_id = ?", projectID.UUID()).Find(&sources).Error; err != nil {
		return nil, err
	}

	return jobSources(sources).ToSpec(), nil
}

func (repo *jobSourceRepository) GetByResourceURN(ctx context.Context, resourceURN string) ([]models.JobSource, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	var sources []JobSource
	if err := repo.db.WithContext(ctx).Where("resource_urn = ?", resourceURN).Find(&sources).Error; err != nil {
		return nil, err
	}

	return jobSources(sources).ToSpec(), nil
}

func (repo *jobSourceRepository) GetResourceURNsPerJobID(ctx context.Context, projectID models.ProjectID) (map[uuid.UUID][]string, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	var sources []JobSource
	if err := repo.db.WithContext(ctx).Where("project_id = ?", projectID.UUID()).Find(&sources).Error; err != nil {
		return nil, err
	}

	resourceURNsPerJobID := make(map[uuid.UUID][]string)
	for _, source := range sources {
		resourceURNsPerJobID[source.JobID] = append(resourceURNsPerJobID[source.JobID], source.ResourceURN)
	}

	return resourceURNsPerJobID, nil
}

func (repo *jobSourceRepository) DeleteByJobID(ctx context.Context, jobID uuid.UUID) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return repo.delete(ctx, repo.db, jobID)
}

func (*jobSourceRepository) delete(ctx context.Context, db *gorm.DB, jobID uuid.UUID) error {
	return db.WithContext(ctx).Where("job_id = ?", jobID).Delete(&JobSource{}).Error
}

func NewJobSourceRepository(db *gorm.DB) store.JobSourceRepository {
	return &jobSourceRepository{
		db: db,
	}
}
