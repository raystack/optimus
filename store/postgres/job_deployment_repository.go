package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type JobDeployment struct {
	ID        uuid.UUID `gorm:"not null" json:"id"`
	ProjectID uuid.UUID `gorm:"not null" json:"project_id"`
	Project   Project   `gorm:"foreignKey:ProjectID"`

	Status  string `gorm:"not null;default:null"`
	Details datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

type jobDeploymentRepository struct {
	db *gorm.DB
}

func (d JobDeployment) ToSpec() (models.JobDeployment, error) {
	projectSpec := d.Project.ToSpec()

	jobDeploymentDetail := models.JobDeploymentDetail{}
	if err := json.Unmarshal(d.Details, &jobDeploymentDetail); err != nil {
		return models.JobDeployment{}, err
	}

	return models.JobDeployment{
		ID:        models.DeploymentID(d.ID),
		Project:   projectSpec,
		Status:    models.JobDeploymentStatus(d.Status),
		Details:   jobDeploymentDetail,
		CreatedAt: d.CreatedAt,
		UpdatedAt: d.UpdatedAt,
	}, nil
}

func (JobDeployment) FromSpec(deployment models.JobDeployment) (JobDeployment, error) {
	details, err := json.Marshal(deployment.Details)
	if err != nil {
		return JobDeployment{}, err
	}

	return JobDeployment{
		ID:        deployment.ID.UUID(),
		ProjectID: deployment.Project.ID.UUID(),
		Status:    deployment.Status.String(),
		Details:   details,
	}, nil
}

func (repo *jobDeploymentRepository) Save(ctx context.Context, deploymentSpec models.JobDeployment) error {
	deployment, err := JobDeployment{}.FromSpec(deploymentSpec)
	if err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Create(&deployment).Error
}

func (repo *jobDeploymentRepository) Update(ctx context.Context, deploymentSpec models.JobDeployment) error {
	deploymentToUpdate, err := JobDeployment{}.FromSpec(deploymentSpec)
	if err != nil {
		return err
	}

	var d JobDeployment
	if err := repo.db.WithContext(ctx).Where("id = ?", deploymentSpec.ID.UUID()).Find(&d).Error; err != nil {
		return errors.New("could not update non-existing job deployment")
	}
	d.Status = deploymentToUpdate.Status
	d.Details = deploymentToUpdate.Details
	return repo.db.WithContext(ctx).Save(&d).Error
}

func (repo *jobDeploymentRepository) GetByID(ctx context.Context, deployID models.DeploymentID) (models.JobDeployment, error) {
	var jobDeployment JobDeployment
	if err := repo.db.WithContext(ctx).Preload("Project").Where("job_deployment.id = ?", deployID.UUID()).First(&jobDeployment).Error; err != nil {
		return models.JobDeployment{}, err
	}
	return jobDeployment.ToSpec()
}

func (repo *jobDeploymentRepository) GetByStatusAndProjectID(ctx context.Context, status models.JobDeploymentStatus, projectID models.ProjectID) (models.JobDeployment, error) {
	var jobDeployment JobDeployment
	if err := repo.db.WithContext(ctx).Preload("Project").Where("status = ? and project_id = ?", status.String(), projectID.UUID()).First(&jobDeployment).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobDeployment{}, store.ErrResourceNotFound
		}
		return models.JobDeployment{}, err
	}
	return jobDeployment.ToSpec()
}

func (repo *jobDeploymentRepository) GetAndUpdateExecutableRequests(ctx context.Context, limit int) (jobDeploymentSpecs []models.JobDeployment, err error) {
	err = repo.db.Transaction(func(tx *gorm.DB) error {
		var jobDeployments []JobDeployment
		if err := tx.WithContext(ctx).Preload("Project").Where("status=? and project_id not in (select project_id from job_deployment where status=?)",
			models.JobDeploymentStatusInQueue.String(), models.JobDeploymentStatusInProgress.String()).Order("created_at ASC").Limit(limit).Find(&jobDeployments).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return store.ErrResourceNotFound
			}
			return err
		}

		for _, jobDeployment := range jobDeployments {
			jobDeploymentSpec, err := jobDeployment.ToSpec()
			if err != nil {
				return err
			}
			jobDeployment.Status = models.JobDeploymentStatusInProgress.String()
			if err := tx.WithContext(ctx).Save(&jobDeployment).Error; err != nil {
				return err
			}
			jobDeploymentSpec.Status = models.JobDeploymentStatusInProgress
			jobDeploymentSpecs = append(jobDeploymentSpecs, jobDeploymentSpec)
		}

		return nil
	})

	return jobDeploymentSpecs, err
}

func (repo *jobDeploymentRepository) GetByStatus(ctx context.Context, status models.JobDeploymentStatus) ([]models.JobDeployment, error) {
	var jobDeployments []JobDeployment
	if err := repo.db.WithContext(ctx).Preload("Project").Where("status = ?", status.String()).Find(&jobDeployments).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, store.ErrResourceNotFound
		}
		return nil, err
	}

	var jobDeploymentSpecs []models.JobDeployment
	for _, deployment := range jobDeployments {
		deploymentSpec, err := deployment.ToSpec()
		if err != nil {
			return nil, err
		}
		jobDeploymentSpecs = append(jobDeploymentSpecs, deploymentSpec)
	}
	return jobDeploymentSpecs, nil
}

func NewJobDeploymentRepository(db *gorm.DB) *jobDeploymentRepository {
	return &jobDeploymentRepository{
		db: db,
	}
}
