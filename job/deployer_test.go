package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestDeployer(t *testing.T) {
	t.Run("Deploy", func(t *testing.T) {
		ctx := context.Background()
		projectSpec := models.ProjectSpec{
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
			Status:  models.JobDeploymentStatusInQueue,
		}
		jobDeploymentInProgress := models.JobDeployment{
			ID:      jobDeployment.ID,
			Project: jobDeployment.Project,
			Status:  models.JobDeploymentStatusInProgress,
		}
		errorMsg := "internal error"

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
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
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
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					TotalSuccess: 2,
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
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

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobID1 := uuid.New()
			jobID2 := uuid.New()
			jobID3 := uuid.New()

			externalProjectJob := models.JobSpec{
				Version: 1,
				ID:      jobID3,
				Name:    "test-3",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task:          models.JobSpecTask{},
				NamespaceSpec: namespaceSpec3,
			}

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
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						externalProjectJob.Name: {
							Project: &externalProjectSpec,
							Job:     &externalProjectJob,
							Type:    models.JobSpecDependencyTypeInter,
						},
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						externalProjectJob.Name: {
							Project: &externalProjectSpec,
							Job:     &externalProjectJob,
							Type:    models.JobSpecDependencyTypeInter,
						},
					},
					NamespaceSpec: namespaceSpec2,
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
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					Dependencies: map[string]models.JobSpecDependency{
						externalProjectJob.Name: {
							Project: &externalProjectSpec,
							Job:     &externalProjectJob,
							Type:    models.JobSpecDependencyTypeInter,
						},
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					TotalSuccess: 2,
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)

			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})
		t.Run("should able to deploy jobs with hooks successfully", func(t *testing.T) {
			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)

			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

			jobDeploymentRepo := new(mock.JobDeploymentRepository)
			defer jobDeploymentRepo.AssertExpectations(t)

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
					Task: models.JobSpecTask{},
					Hooks: []models.JobSpecHook{
						{
							Config:    nil,
							Unit:      &models.Plugin{Base: hookUnit1},
							DependsOn: nil,
						},
						{
							Config:    nil,
							Unit:      &models.Plugin{Base: hookUnit2},
							DependsOn: nil,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Hooks: []models.JobSpecHook{
						{
							Config:    nil,
							Unit:      &models.Plugin{Base: hookUnit1},
							DependsOn: nil,
						},
						{
							Config:    nil,
							Unit:      &models.Plugin{Base: hookUnit2},
							DependsOn: nil,
						},
					},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecHooksResolved := []models.JobSpecHook{
				{
					Config:    nil,
					Unit:      &models.Plugin{Base: hookUnit1},
					DependsOn: nil,
				},
				{
					Config:    nil,
					Unit:      &models.Plugin{Base: hookUnit2},
					DependsOn: []*models.JobSpecHook{&jobSpecsBase[0].Hooks[0]},
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:  models.JobSpecTask{},
					Hooks: jobSpecHooksResolved,
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
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
					Hooks: jobSpecHooksResolved,
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					TotalSuccess: 2,
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return(jobSpecHooksResolved).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Nil(t, err)
		})

		t.Run("should fail when unable to update job deployment status to in progress", func(t *testing.T) {
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

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, errorMsg, err.Error())
		})

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

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return([]models.JobSpec{}, errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, errorMsg, err.Error())
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
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return([]models.JobSpec{}, errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, errorMsg, err.Error())
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
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
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
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					TotalSuccess: 1,
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec1.Name).Return(namespaceSpec1, nil)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec2.Name).Return(namespaceSpec2, nil)
			deployError := errors.New(errorMsg)
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(models.JobDeploymentDetail{}, deployError)

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentSucceed).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, &multierror.Error{Errors: []error{deployError}}, err)
		})
		t.Run("should fail when unable to update job deployments status to success", func(t *testing.T) {
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
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
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
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentSucceed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusSucceed,
				Details: models.JobDeploymentDetail{
					TotalSuccess: 2,
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentSucceed).Return(errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to update job deployments status to failed", func(t *testing.T) {
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
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterJobDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobSpecsAfterHookDependencyEnrich := []models.JobSpec{
				{
					Version: 1,
					ID:      jobID1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec2,
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
					Dependencies: map[string]models.JobSpecDependency{
						jobSpecsBase[1].Name: {
							Project: &projectSpec,
							Job:     &jobSpecsBase[1],
							Type:    models.JobSpecDependencyTypeIntra,
						},
					},
					NamespaceSpec: namespaceSpec1,
				},
				{
					Version: 1,
					ID:      jobID2,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 9000,
					},
					NamespaceSpec: namespaceSpec2,
				},
			}
			jobDeploymentDetailNamespace1 := models.JobDeploymentDetail{
				TotalSuccess: 1,
			}
			jobDeploymentDetailNamespace2 := models.JobDeploymentDetail{
				TotalSuccess: 0,
				Failures: []models.JobDeploymentFailure{
					{
						JobName: "job-a",
						Message: errorMsg,
					},
				},
			}
			jobDeploymentFailed := models.JobDeployment{
				ID:      jobDeployment.ID,
				Project: jobDeployment.Project,
				Status:  models.JobDeploymentStatusFailed,
				Details: models.JobDeploymentDetail{
					TotalSuccess: 1,
					Failures: []models.JobDeploymentFailure{
						{
							JobName: "job-a",
							Message: errorMsg,
						},
					},
				},
			}

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentInProgress).Return(nil).Once()

			dependencyResolver.On("FetchJobSpecsWithJobDependencies", ctx, projectSpec).Return(jobSpecsAfterJobDependencyEnrich, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}).Return(jobDeploymentDetailNamespace1, nil).Once()
			batchScheduler.On("DeployJobsVerbose", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}).Return(jobDeploymentDetailNamespace2, nil).Once()

			jobDeploymentRepo.On("UpdateByID", ctx, jobDeploymentFailed).Return(errors.New(errorMsg)).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, batchScheduler, jobDeploymentRepo, namespaceService)
			err := deployer.Deploy(ctx, jobDeployment)

			assert.Equal(t, errorMsg, err.Error())
		})
	})
}
