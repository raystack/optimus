package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestDeployer(t *testing.T) {
	t.Run("Deploy", func(t *testing.T) {
		ctx := context.Background()
		log := log.NewNoop()
		projectSpec := models.ProjectSpec{
			ID:   models.ProjectID(uuid.New()),
			Name: "a-data-project",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
		}

		namespaceSpec1 := models.NamespaceSpec{
			ID:   uuid.New(),
			Name: "namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		namespaceSpec2 := models.NamespaceSpec{
			ID:   uuid.New(),
			Name: "namespace-2",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		jobDeployment := models.JobDeployment{
			ID:      models.DeploymentID(uuid.New()),
			Project: projectSpec,
			Status:  models.JobDeploymentStatusInProgress,
		}
		schedule := models.JobSpecSchedule{
			StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
			Interval:  "@daily",
		}
		errorMsg := "internal error"
		listOptions := models.SchedulerListOptions{OnlyName: true}

		t.Run("should failed when unable to GetJobSpecsWithDependencies", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return([]models.JobSpec{}, nil, errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, err.Error(), errorMsg)
		})

		t.Run("should fail when unable to resolve priority", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobSpecs := []models.JobSpec{}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecs, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecs, nil).Return(jobSpecs, errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.EqualError(t, err, errorMsg)
		})

		t.Run("should fail when unable to get namespace spec", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobID1 := uuid.New()
			jobSpecs := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  make(map[string]models.JobSpecDependency),
				},
			}
			jobSpecsWithPriorityWeight := jobSpecs
			jobSpecsWithPriorityWeight[0].Task = models.JobSpecTask{Priority: 1000}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecs, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecs, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			jobDeploymentRepo.On("Update", ctx, tMock.Anything).Return(nil)

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Error(t, err)
		})

		t.Run("should fail when unable to deploy jobs", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobID1 := uuid.New()
			jobSpecs := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  make(map[string]models.JobSpecDependency),
				},
			}

			jobSpecsWithPriorityWeight := jobSpecs
			jobSpecsWithPriorityWeight[0].Task = models.JobSpecTask{Priority: 1000}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecs, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecs, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)

			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, jobSpecsWithPriorityWeight).Return(models.JobDeploymentDetail{}, errors.New(errorMsg))

			jobDeploymentRepo.On("Update", ctx, tMock.Anything).Return(nil)

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Error(t, err)
		})

		t.Run("should fail when unable to update job deployments status", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobID1 := uuid.New()
			jobSpecs := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  make(map[string]models.JobSpecDependency),
				},
			}

			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecs[0].Name},
			}

			jobSpecsWithPriorityWeight := jobSpecs
			jobSpecsWithPriorityWeight[0].Task = models.JobSpecTask{Priority: 1000}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecs, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecs, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)

			batchScheduler.On("ListJobs", ctx, namespaceSpec1.Name, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec1.ID.String(), namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.Name, namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.ID.String(), namespaceSpec1).Return(nil).Once()

			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, jobSpecsWithPriorityWeight).Return(models.JobDeploymentDetail{}, nil)

			jobDeploymentRepo.On("Update", ctx, tMock.Anything).Return(errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Error(t, err)
		})

		t.Run("should return error when unable to list of deployment storage bucket", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobID1 := uuid.New()
			jobID2 := uuid.New()

			jobSpecsBase := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					ResourceDestination: "resource-a",
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsWithDependency := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  job1Dependencies,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobSpecsWithPriorityWeight := []models.JobSpec{
				{
					Version:  1,
					ID:       jobID1,
					Name:     "test",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 10000,
					},
					Dependencies:  job1Dependencies,
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:  1,
					ID:       jobID2,
					Name:     "test-2",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					SuccessCount: 2,
				},
			}
			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[0].Name},
			}
			schedulerJobNamespace2 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[1].Name},
			}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecsWithDependency, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecsWithDependency, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsWithPriorityWeight[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsWithPriorityWeight[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			batchScheduler.On("ListJobs", ctx, namespaceSpec1.Name, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil)
			batchScheduler.On("ListJobs", ctx, namespaceSpec1.ID.String(), namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil)

			batchScheduler.On("ListJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, listOptions).Return(schedulerJobNamespace2, errors.New(errorMsg))

			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.Name, namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.ID.String(), namespaceSpec1).Return(nil).Once()

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Contains(t, err.Error(), errorMsg)
		})

		t.Run("should return error when unable to clean deployment storage", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobID1 := uuid.New()
			jobID2 := uuid.New()

			jobSpecsBase := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					ResourceDestination: "resource-a",
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsWithDependency := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  job1Dependencies,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobSpecsWithPriorityWeight := []models.JobSpec{
				{
					Version:  1,
					ID:       jobID1,
					Name:     "test",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 10000,
					},
					Dependencies:  job1Dependencies,
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:  1,
					ID:       jobID2,
					Name:     "test-2",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					SuccessCount: 2,
				},
			}
			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[0].Name},
			}
			unusedFileName := "unused_file"
			schedulerJobNamespace2 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[1].Name},
				{Name: unusedFileName},
			}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecsWithDependency, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecsWithDependency, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsWithPriorityWeight[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsWithPriorityWeight[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			batchScheduler.On("ListJobs", ctx, namespaceSpec1.Name, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec1.ID.String(), namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			//batchScheduler.On("ListJobs", ctx, namespaceSpec2.Name, namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()

			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.Name, namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.ID.String(), namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, []string{unusedFileName}, nil).Return(errors.New(errorMsg))

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Contains(t, err.Error(), errorMsg)
		})

		t.Run("should able to deploy jobs successfully", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobID1 := uuid.New()
			jobID2 := uuid.New()

			jobSpecsBase := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					ResourceDestination: "resource-a",
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsWithDependency := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  job1Dependencies,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobSpecsWithPriorityWeight := []models.JobSpec{
				{
					Version:  1,
					ID:       jobID1,
					Name:     "test",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 10000,
					},
					Dependencies:  job1Dependencies,
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:  1,
					ID:       jobID2,
					Name:     "test-2",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					SuccessCount: 2,
				},
			}
			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[0].Name},
			}
			schedulerJobNamespace2 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[1].Name},
			}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecsWithDependency, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecsWithDependency, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsWithPriorityWeight[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsWithPriorityWeight[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			batchScheduler.On("ListJobs", ctx, namespaceSpec1.Name, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec1.ID.String(), namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.Name, namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()

			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.Name, namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.ID.String(), namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec2.Name, namespaceSpec2).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec2.ID.String(), namespaceSpec2).Return(nil).Once()

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})

		t.Run("should able to deploy jobs with cleanup successfully", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobID1 := uuid.New()
			jobID2 := uuid.New()

			jobSpecsBase := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					ResourceDestination: "resource-a",
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsWithDependency := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  job1Dependencies,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobSpecsWithPriorityWeight := []models.JobSpec{
				{
					Version:  1,
					ID:       jobID1,
					Name:     "test",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 10000,
					},
					Dependencies:  job1Dependencies,
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:  1,
					ID:       jobID2,
					Name:     "test-2",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					SuccessCount: 2,
				},
			}
			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[0].Name},
			}
			unusedFileName := "unused_file"
			schedulerJobNamespace2 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[1].Name},
				{Name: unusedFileName},
			}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecsWithDependency, nil, nil)

			priorityResolver.On("Resolve", ctx, jobSpecsWithDependency, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsWithPriorityWeight[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsWithPriorityWeight[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			batchScheduler.On("ListJobs", ctx, namespaceSpec1.Name, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec1.ID.String(), namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.Name, namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()

			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.Name, namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.ID.String(), namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec2.Name, namespaceSpec2).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec2.ID.String(), namespaceSpec2).Return(nil).Once()

			batchScheduler.On("DeleteJobs", ctx, namespaceSpec2.Name, namespaceSpec2, []string{unusedFileName}, nil).Return(nil)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, []string{unusedFileName}, nil).Return(nil)

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})

		t.Run("should able to deploy jobs with unknown dependencies info provided successfully", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobID1 := uuid.New()
			jobID2 := uuid.New()

			jobSpecsBase := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					ResourceDestination: "resource-a",
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsWithDependency := []models.JobSpec{
				{
					Version:       1,
					ID:            jobID1,
					Name:          "test",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec1,
					Dependencies:  job1Dependencies,
				},
				{
					Version:             1,
					ID:                  jobID2,
					Name:                "test-2",
					Owner:               "optimus",
					Schedule:            schedule,
					Task:                models.JobSpecTask{},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			jobSpecsWithPriorityWeight := []models.JobSpec{
				{
					Version:  1,
					ID:       jobID1,
					Name:     "test",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 10000,
					},
					Dependencies:  job1Dependencies,
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version:  1,
					ID:       jobID2,
					Name:     "test-2",
					Owner:    "optimus",
					Schedule: schedule,
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec:       namespaceSpec2,
					Dependencies:        map[string]models.JobSpecDependency{},
					ResourceDestination: "resource-a",
				},
			}
			unknownDependencies := []models.UnknownDependency{
				{
					JobName:               jobSpecsWithDependency[0].Name,
					DependencyProjectName: "unknown-project",
					DependencyJobName:     "unknown-job",
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusFailed,
				Details: models.JobDeploymentDetail{
					SuccessCount: 2,
					UnknownDependenciesPerJobName: map[string][]string{
						unknownDependencies[0].JobName: {
							unknownDependencies[0].DependencyProjectName + "/" + unknownDependencies[0].DependencyJobName,
						},
					},
				},
			}
			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[0].Name},
			}
			schedulerJobNamespace2 := []models.Job{
				{Name: jobSpecsWithPriorityWeight[1].Name},
			}

			dependencyResolver.On("GetJobSpecsWithDependencies", ctx, projectSpec.ID).Return(jobSpecsWithDependency, unknownDependencies, nil)

			priorityResolver.On("Resolve", ctx, jobSpecsWithDependency, nil).Return(jobSpecsWithPriorityWeight, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsWithPriorityWeight[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsWithPriorityWeight[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			batchScheduler.On("ListJobs", ctx, namespaceSpec1.Name, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec1.ID.String(), namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.Name, namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2.ID.String(), namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()

			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.Name, namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec1.ID.String(), namespaceSpec1).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec2.Name, namespaceSpec2).Return(nil).Once()
			batchScheduler.On("DeleteDagsDirectoryIfEmpty", ctx, namespaceSpec2.ID.String(), namespaceSpec2).Return(nil).Once()
			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})
	})
}
