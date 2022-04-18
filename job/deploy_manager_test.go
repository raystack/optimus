package job_test

import (
	"context"
	"errors"
	"github.com/odpf/optimus/store"
	"testing"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestDeployManager(t *testing.T) {
	t.Run("Deploy", func(t *testing.T) {
		ctx := context.Background()
		log := log.NewNoop()
		projectSpec := models.ProjectSpec{
			Name: "a-data-project",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}
		errorMsg := "internal error"

		t.Run("should failed when unable to get job deployment using status", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})

		t.Run("should failed when unable to create new deployment ID", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(uuid.Nil, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})

		t.Run("should failed when unable to save new deployment", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}

			newJobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusCreated,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(newJobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, newJobDeployment).Return(errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})
	})
}
