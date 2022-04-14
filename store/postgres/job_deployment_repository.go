package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/odpf/optimus/store"
	"gorm.io/datatypes"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type JobDeployment struct {
	ID        uuid.UUID `gorm:"not null" json:"id"`
	ProjectID uuid.UUID `gorm:"not null" json:"project_id"`
	Project   Project   `gorm:"foreignKey:ProjectID"`

	Status  string `gorm:"not null"`
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
		ID:      models.DeploymentID(d.ID),
		Project: projectSpec,
		Status:  models.JobDeploymentStatus(d.Status),
		Details: jobDeploymentDetail,
	}, nil
}

func (d JobDeployment) FromSpec(deployment models.JobDeployment) (JobDeployment, error) {
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

func (repo *jobDeploymentRepository) UpdateByID(ctx context.Context, deploymentSpec models.JobDeployment) error {
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
	if err := repo.db.WithContext(ctx).Preload("Project").Where("job_deployment.id = ?", deployID).First(&jobDeployment).Error; err != nil {
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

func NewJobDeploymentRepository(db *gorm.DB) *jobDeploymentRepository {
	return &jobDeploymentRepository{
		db: db,
	}
}
