package postgres

import (
	"context"
	"encoding/json"
	"errors"
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
		ID:      d.ID,
		Project: projectSpec,
		Status:  d.Status,
		Details: jobDeploymentDetail,
	}, nil
}

func (d JobDeployment) FromSpec(deployment models.JobDeployment) (JobDeployment, error) {
	details, err := json.Marshal(deployment.Details)
	if err != nil {
		return JobDeployment{}, err
	}

	return JobDeployment{
		ProjectID: deployment.Project.ID.UUID(),
		Status:    deployment.Status,
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
	if err := repo.db.WithContext(ctx).Where("id = ?", deploymentSpec.ID).Find(&d).Error; err != nil {
		return errors.New("could not update non-existing job deployment")
	}
	d.Status = deploymentToUpdate.Status
	d.Details = deploymentToUpdate.Details
	return repo.db.WithContext(ctx).Save(&d).Error
}

func (repo *jobDeploymentRepository) GetByID(ctx context.Context, deployID uuid.UUID) (models.JobDeployment, error) {
	var jobDeployment JobDeployment
	if err := repo.db.WithContext(ctx).Preload("Project").Where("job_deployment.id = ?", deployID).First(&jobDeployment).Error; err != nil {
		return models.JobDeployment{}, err
	}
	return jobDeployment.ToSpec()
}

func (repo *jobDeploymentRepository) GetByStatusAndProjectID(ctx context.Context, status string, projectID models.ProjectID) (models.JobDeployment, error) {
	var jobDeployment JobDeployment
	if err := repo.db.WithContext(ctx).Preload("Project").Where("status = ? and project_id = ?", status, projectID).First(&jobDeployment).Error; err != nil {
		return models.JobDeployment{}, err
	}
	return jobDeployment.ToSpec()
}

func NewJobDeploymentRepository(db *gorm.DB) *jobDeploymentRepository {
	return &jobDeploymentRepository{
		db: db,
	}
}
