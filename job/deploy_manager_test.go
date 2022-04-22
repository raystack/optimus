package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	testifyMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/config"
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

			deployManagerConfig := config.Deployer{
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

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, existingJobDeployment.ID, deployID)
			assert.Nil(t, err)
		})
		t.Run("should fail when unable to get job deployment using status", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to create new deployment ID", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(uuid.Nil, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to save new job deployment", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}
			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusInQueue,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(jobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, jobDeployment).Return(errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, models.DeploymentID{}, deployID)
			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should be able to return new deployment ID if no similar existing request found", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    1,
				WorkerTimeout: 300,
				QueueCapacity: 10,
			}
			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusInQueue,
			}

			jobDeploymentRepository.On("GetByStatusAndProjectID", ctx, models.JobDeploymentStatusInQueue, projectSpec.ID).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			uuidProvider.On("NewUUID").Return(jobDeployment.ID.UUID(), nil)

			jobDeploymentRepository.On("Save", ctx, jobDeployment).Return(nil)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployID, err := deployManager.Deploy(ctx, projectSpec)

			assert.Equal(t, jobDeployment.ID, deployID)
			assert.Nil(t, err)
		})
	})

	t.Run("GetStatus", func(t *testing.T) {
		t.Run("should be able to return job deployment", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			expectedJobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusInProgress,
			}

			jobDeploymentRepository.On("GetByID", ctx, expectedJobDeployment.ID).Return(expectedJobDeployment, nil)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			jobDeployment, err := deployManager.GetStatus(ctx, expectedJobDeployment.ID)

			assert.Nil(t, err)
			assert.Equal(t, expectedJobDeployment, jobDeployment)
		})
		t.Run("should fail when unable to return job deployment", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			jobDeploymentID := models.DeploymentID(uuid.New())

			jobDeploymentRepository.On("GetByID", ctx, jobDeploymentID).Return(models.JobDeployment{}, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			jobDeployment, err := deployManager.GetStatus(ctx, jobDeploymentID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.JobDeployment{}, jobDeployment)
		})
	})

	t.Run("Assign", func(t *testing.T) {
		t.Run("should be able to assign deployer a new request", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			jobDeployment := models.JobDeployment{
				ID:      models.DeploymentID(uuid.New()),
				Project: projectSpec,
				Status:  models.JobDeploymentStatusInProgress,
			}

			jobDeploymentRepository.On("GetByStatus", testifyMock.Anything, models.JobDeploymentStatusInProgress).Return([]models.JobDeployment{}, nil)
			jobDeploymentRepository.On("GetFirstExecutableRequest", testifyMock.Anything).Return(jobDeployment, nil)
			deployer.On("Deploy", testifyMock.Anything, jobDeployment).Return(nil).Maybe()

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployManager.Assign()
		})
		t.Run("should be able to cancel timed out deployments", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			timedOutDeployments := []models.JobDeployment{
				{
					ID:        models.DeploymentID(uuid.New()),
					Project:   projectSpec,
					Status:    models.JobDeploymentStatusInProgress,
					CreatedAt: time.Now().Add(time.Hour * -6),
				},
			}

			jobDeploymentRepository.On("GetByStatus", testifyMock.Anything, models.JobDeploymentStatusInProgress).Return(timedOutDeployments, nil)
			timedOutDeployments[0].Status = models.JobDeploymentStatusCancelled
			jobDeploymentRepository.On("UpdateByID", testifyMock.Anything, timedOutDeployments[0]).Return(nil)
			jobDeploymentRepository.On("GetFirstExecutableRequest", testifyMock.Anything).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployManager.Assign()
		})
		t.Run("should not proceed on cancelling timed out deployments when unable to get in progress deployments", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}

			jobDeploymentRepository.On("GetByStatus", testifyMock.Anything, models.JobDeploymentStatusInProgress).Return([]models.JobDeployment{}, errors.New(errorMsg))
			jobDeploymentRepository.On("GetFirstExecutableRequest", testifyMock.Anything).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployManager.Assign()
		})
		t.Run("should proceed with assigning when unable to mark timed out deployments as cancelled", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			timedOutDeployments := []models.JobDeployment{
				{
					ID:        models.DeploymentID(uuid.New()),
					Project:   projectSpec,
					Status:    models.JobDeploymentStatusInProgress,
					CreatedAt: time.Now().Add(time.Hour * -6),
				},
			}

			jobDeploymentRepository.On("GetByStatus", testifyMock.Anything, models.JobDeploymentStatusInProgress).Return(timedOutDeployments, nil)
			timedOutDeployments[0].Status = models.JobDeploymentStatusCancelled
			jobDeploymentRepository.On("UpdateByID", testifyMock.Anything, timedOutDeployments[0]).Return(errors.New(errorMsg))
			jobDeploymentRepository.On("GetFirstExecutableRequest", testifyMock.Anything).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployManager.Assign()
		})
		t.Run("should not assign if executable requests are not found", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			timedOutDeployments := []models.JobDeployment{
				{
					ID:        models.DeploymentID(uuid.New()),
					Project:   projectSpec,
					Status:    models.JobDeploymentStatusInProgress,
					CreatedAt: time.Now().Add(time.Hour * -6),
				},
			}

			jobDeploymentRepository.On("GetByStatus", testifyMock.Anything, models.JobDeploymentStatusInProgress).Return(timedOutDeployments, nil)
			timedOutDeployments[0].Status = models.JobDeploymentStatusCancelled
			jobDeploymentRepository.On("UpdateByID", testifyMock.Anything, timedOutDeployments[0]).Return(nil)
			jobDeploymentRepository.On("GetFirstExecutableRequest", testifyMock.Anything).Return(models.JobDeployment{}, store.ErrResourceNotFound)

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployManager.Assign()
		})
		t.Run("should not assign if unable to get executable requests", func(t *testing.T) {
			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			uuidProvider := new(mock.UUIDProvider)
			defer uuidProvider.AssertExpectations(t)

			jobDeploymentRepository := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepository.AssertExpectations(t)

			deployManagerConfig := config.Deployer{
				NumWorkers:    3,
				WorkerTimeout: time.Second,
			}
			timedOutDeployments := []models.JobDeployment{
				{
					ID:        models.DeploymentID(uuid.New()),
					Project:   projectSpec,
					Status:    models.JobDeploymentStatusInProgress,
					CreatedAt: time.Now().Add(time.Hour * -6),
				},
			}

			jobDeploymentRepository.On("GetByStatus", testifyMock.Anything, models.JobDeploymentStatusInProgress).Return(timedOutDeployments, nil)
			timedOutDeployments[0].Status = models.JobDeploymentStatusCancelled
			jobDeploymentRepository.On("UpdateByID", testifyMock.Anything, timedOutDeployments[0]).Return(nil)
			jobDeploymentRepository.On("GetFirstExecutableRequest", testifyMock.Anything).Return(models.JobDeployment{}, errors.New(errorMsg))

			deployManager := job.NewDeployManager(log, deployManagerConfig, deployer, uuidProvider, jobDeploymentRepository, nil)
			deployManager.Assign()
		})
	})
}
