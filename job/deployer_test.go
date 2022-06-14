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
		externalProjectSpec := models.ProjectSpec{
			Name: "b-data-project",
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
		namespaceSpec3 := models.NamespaceSpec{
			ID:   uuid.New(),
			Name: "namespace-3",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: externalProjectSpec,
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

		t.Run("should fail when unable to fetch job specs with job dependencies", func(t *testing.T) {
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

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)

			projectJobSpecRepo.On("GetAll", ctx).Return(nil, errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, errorMsg, err.Error())
		})

		t.Run("should fail when unable to fetch job sources", func(t *testing.T) {
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

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)

			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return([]models.JobSource{}, errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Error(t, err)
		})

		t.Run("should fail when unable to fetch job spec of the job source", func(t *testing.T) {
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
					Version:       1,
					ID:            jobID2,
					Name:          "test-2",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSources := []models.JobSource{
				{
					JobID:       jobID1,
					ProjectID:   projectSpec.ID,
					ResourceURN: "resource-a",
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetByDestination", ctx, jobSources[0].ResourceURN).Return(models.JobSpec{}, errors.New(errorMsg))

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Contains(t, err.Error(), errorMsg)
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
			jobSources := []models.JobSource{}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)

			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecs, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			priorityResolver.On("Resolve", ctx, jobSpecs, nil).Return(jobSpecs, errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
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
			emptyJobSpecs := []models.JobSpec{}
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
			jobSources := []models.JobSource{}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)

			projectJobSpecRepo.On("GetAll", ctx).Return(emptyJobSpecs, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			batchScheduler.On("ListJobs", ctx, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()

			priorityResolver.On("Resolve", ctx, emptyJobSpecs, nil).Return(jobSpecs, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			jobDeploymentRepo.On("Update", ctx, tMock.Anything).Return(nil)

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
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
			emptyJobSpecs := []models.JobSpec{}
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
			jobSources := []models.JobSource{}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)

			projectJobSpecRepo.On("GetAll", ctx).Return(emptyJobSpecs, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			priorityResolver.On("Resolve", ctx, emptyJobSpecs, nil).Return(jobSpecs, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)

			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, jobSpecs).Return(models.JobDeploymentDetail{}, errors.New(errorMsg))

			jobDeploymentRepo.On("Update", ctx, tMock.Anything).Return(nil)

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
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
			emptyJobSpecs := []models.JobSpec{}
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
			jobSources := []models.JobSource{}

			listOptions := models.SchedulerListOptions{OnlyName: true}

			schedulerJobNamespace1 := []models.Job{
				{Name: jobSpecs[0].Name},
			}
			schedulerJobNamespace2 := []models.Job{
				{Name: jobSpecs[1].Name},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)

			projectJobSpecRepo.On("GetAll", ctx).Return(emptyJobSpecs, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			priorityResolver.On("Resolve", ctx, emptyJobSpecs, nil).Return(jobSpecs, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)

			batchScheduler.On("ListJobs", ctx, namespaceSpec1, listOptions).Return(schedulerJobNamespace1, nil).Once()
			batchScheduler.On("ListJobs", ctx, namespaceSpec2, listOptions).Return(schedulerJobNamespace2, nil).Once()
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, jobSpecs).Return(models.JobDeploymentDetail{}, nil)

			jobDeploymentRepo.On("Update", ctx, tMock.Anything).Return(errors.New(errorMsg))

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Error(t, err)
		})

		t.Run("should able to deploy jobs with resource dependency successfully", func(t *testing.T) {
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
					Version:       1,
					ID:            jobID2,
					Name:          "test-2",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsAfterDependencyResolution := []models.JobSpec{
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
					Version:       1,
					ID:            jobID2,
					Name:          "test-2",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterPriorityResolution := []models.JobSpec{
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
					NamespaceSpec: namespaceSpec2,
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
			jobSources := []models.JobSource{
				{
					JobID:       jobID1,
					ProjectID:   projectSpec.ID,
					ResourceURN: "resource-a",
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetByDestination", ctx, jobSources[0].ResourceURN).Return(jobSpecsBase[1], nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			dependencyResolver.On("ResolveStaticDependencies", ctx, jobSpecsBase[0], projectSpec, projectJobSpecRepo).Return(job1Dependencies, nil).Once()
			dependencyResolver.On("ResolveStaticDependencies", ctx, jobSpecsBase[1], projectSpec, projectJobSpecRepo).Return(nil, nil).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterDependencyResolution[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterDependencyResolution[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsBase, nil).Return(jobSpecsAfterPriorityResolution, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})

		t.Run("should able to deploy jobs with external project dependency successfully", func(t *testing.T) {
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
				},
			}
			externalJobSpec := models.JobSpec{
				Version: 1,
				ID:      jobID2,
				Name:    "test-2",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task:          models.JobSpecTask{},
				NamespaceSpec: namespaceSpec3,
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				externalJobSpec.Name: {
					Project: &externalProjectSpec,
					Job:     &externalJobSpec,
					Type:    models.JobSpecDependencyTypeInter,
				},
			}
			jobSpecsDependencyEnriched := []models.JobSpec{
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
					Dependencies:  job1Dependencies,
				},
			}
			jobSpecsAfterPriorityResolution := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
					Dependencies:  job1Dependencies,
					NamespaceSpec: namespaceSpec1,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				SuccessCount: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					SuccessCount: 1,
				},
			}
			jobSources := []models.JobSource{
				{
					JobID:       jobID1,
					ProjectID:   projectSpec.ID,
					ResourceURN: "resource-a",
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetByDestination", ctx, jobSources[0].ResourceURN).Return(externalJobSpec, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return(jobSources, nil)

			dependencyResolver.On("ResolveStaticDependencies", ctx, jobSpecsBase[0], projectSpec, projectJobSpecRepo).Return(job1Dependencies, nil).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsDependencyEnriched[0]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsBase, nil).Return(jobSpecsAfterPriorityResolution, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})

		t.Run("should able to deploy jobs with hooks successfully", func(t *testing.T) {
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

			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)

			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

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
					Version:       1,
					ID:            jobID2,
					Name:          "test-2",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			job1Dependencies := map[string]models.JobSpecDependency{
				jobSpecsBase[1].Name: {
					Project: &projectSpec,
					Job:     &jobSpecsBase[1],
					Type:    models.JobSpecDependencyTypeIntra,
				},
			}
			jobSpecsDependenciesEnriched := []models.JobSpec{
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
					Version:       1,
					ID:            jobID2,
					Name:          "test-2",
					Owner:         "optimus",
					Schedule:      schedule,
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterPriorityResolution := []models.JobSpec{
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
					NamespaceSpec: namespaceSpec2,
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
			jobSpecHooks := []models.JobSpecHook{
				{
					Unit: &models.Plugin{Base: hookUnit1},
				},
				{
					Unit: &models.Plugin{Base: hookUnit2},
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			jobSourceRepo.On("GetAll", ctx, projectSpec.ID).Return([]models.JobSource{}, nil)

			dependencyResolver.On("ResolveStaticDependencies", ctx, jobSpecsBase[0], projectSpec, projectJobSpecRepo).Return(job1Dependencies, nil).Once()
			dependencyResolver.On("ResolveStaticDependencies", ctx, jobSpecsBase[1], projectSpec, projectJobSpecRepo).Return(nil, nil).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsDependenciesEnriched[0]).Return(jobSpecHooks).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsDependenciesEnriched[1]).Return(jobSpecHooks).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsBase, nil).Return(jobSpecsAfterPriorityResolution, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("Update", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(log, dependencyResolver, priorityResolver, namespaceService, jobDeploymentRepo, projJobSpecRepoFac, jobSourceRepo, batchScheduler)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})
	})
}
