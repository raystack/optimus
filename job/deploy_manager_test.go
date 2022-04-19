package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	mockTestify "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

func TestDeployManager(t *testing.T) {
	ctx := context.Background()
	log := log.NewNoop()
	projectSpec := models.ProjectSpec{
		Name: "a-data-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	errorMsg := "internal error"
	t.Run("Deploy", func(t *testing.T) {
		t.Run("should return existing request ID if similar request is in queue", func(t *testing.T) {
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
			existingJobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusInQueue,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(existingJobDeployment, nil)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, existingJobDeployment.ID, deployID)
			assert.Nil(t, err)
		})
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
		t.Run("should failed when unable to update status deployment to queue", func(t *testing.T) {
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

			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusCreated,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(jobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, jobDeployment).Return(nil)

			// Deploy is run asynchronously. Can be done before or after deploy manager returns.
			deployer.On("Deploy", mockTestify.Anything, jobDeployment).Return(nil).Maybe()

			jobDeployment.Status = models.JobDeploymentStatusInQueue
			jobDeploymentRepository.On("UpdateByID", ctx, jobDeployment).Return(errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should return new deployment request ID when able to push to queue and update the status", func(t *testing.T) {
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

			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusCreated,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(jobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, jobDeployment).Return(nil)

			// Deploy is run asynchronously. Can be done before or after deploy manager returns.
			deployer.On("Deploy", mockTestify.Anything, jobDeployment).Return(nil).Maybe()

			jobDeployment.Status = models.JobDeploymentStatusInQueue
			jobDeploymentRepository.On("UpdateByID", ctx, jobDeployment).Return(nil)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, jobDeployment.ID, deployID)
			assert.Nil(t, err)
		})
		t.Run("should failed when unable to push to queue", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    0,
				WorkerTimeout: 300,
				QueueCapacity: 0,
			}

			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusCreated,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(jobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, jobDeployment).Return(nil)

			jobDeployment.Status = models.JobDeploymentStatusCancelled
			jobDeploymentRepository.On("UpdateByID", ctx, jobDeployment).Return(nil)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, "unable to push deployment request to queue", err.Error())
		})
		t.Run("should return error when unable to mark job deployment as cancelled", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    0,
				WorkerTimeout: 300,
				QueueCapacity: 0,
			}

			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusCreated,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(jobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, jobDeployment).Return(nil)

			jobDeployment.Status = models.JobDeploymentStatusCancelled
			jobDeploymentRepository.On("UpdateByID", ctx, jobDeployment).Return(errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})
	})

	t.Run("GetStatus", func(t *testing.T) {
		t.Run("should able to return job deployment", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			expectedJobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusInProgress,
			}

			jobDeploymentRepository.On("GetByID", ctx, expectedJobDeployment.ID).Return(expectedJobDeployment, nil)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			jobDeployment, err := deployManager.GetStatus(ctx, expectedJobDeployment.ID)

			assert.Nil(t, err)
			assert.Equal(t, expectedJobDeployment, jobDeployment)
		})
		t.Run("should failed when unable to return job deployment", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := job.DeployManagerConfig{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			jobDeploymentID := models.DeploymentID(uuid.New())

			jobDeploymentRepository.On("GetByID", ctx, jobDeploymentID).Return(models.JobDeployment{}, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)
			jobDeployment, err := deployManager.GetStatus(ctx, jobDeploymentID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.JobDeployment{}, jobDeployment)
		})
	})

	t.Run("Close", func(t *testing.T) {
		deployer := new(mock.Deployer)
		defer deployer.AssertExpectations(t)

		uuidProvider := new(mock.UUIDProvider)
		defer uuidProvider.AssertExpectations(t)

		jobDeploymentRepository := new(mock.JobDeploymentRepository)
		defer jobDeploymentRepository.AssertExpectations(t)

		deployManagerConfig := job.DeployManagerConfig{
			NumWorkers:    3,
			WorkerTimeout: time.Second,
		}

		deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository)

		err := deployManager.Close()
		assert.Nil(t, err)
	})
}
