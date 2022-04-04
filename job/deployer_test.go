package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
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

			jobDependenciesPairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpecsBase[0].ID,
					DependentJobID:   jobSpecsBase[1].ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return(jobDependenciesPairs, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}, nil).Return(nil).Once()
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}, nil).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

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

			jobDependenciesPairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpecsBase[0].ID,
					DependentJobID:   jobSpecsBase[1].ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobSpecsBase[1].ID,
					DependentJobID:   externalProjectJob.ID,
					DependentProject: externalProjectSpec,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo).Once()
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return(jobDependenciesPairs, nil)

			projJobSpecRepoFac.On("New", externalProjectSpec).Return(projectJobSpecRepo).Once()
			projectJobSpecRepo.On("GetByIDs", ctx, []uuid.UUID{externalProjectJob.ID}).Return([]models.JobSpec{externalProjectJob}, nil)

			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}, nil).Return(nil).Once()
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}, nil).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

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

			hookUnit1 := new(mock.BasePlugin)
			defer hookUnit1.AssertExpectations(t)

			hookUnit2 := new(mock.BasePlugin)
			defer hookUnit2.AssertExpectations(t)

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

			jobDependenciesPairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpecsBase[0].ID,
					DependentJobID:   jobSpecsBase[1].ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return(jobDependenciesPairs, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return(jobSpecHooksResolved).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}, nil).Return(nil).Once()
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}, nil).Return(nil).Once()

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

			assert.Nil(t, err)
		})
		t.Run("should fail when unable to get job specs", func(t *testing.T) {
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

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to fetch job dependencies", func(t *testing.T) {
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

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return([]models.JobIDDependenciesPair{}, errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to fetch external project job spec", func(t *testing.T) {
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

			jobDependenciesPairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpecsBase[0].ID,
					DependentJobID:   jobSpecsBase[1].ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobSpecsBase[1].ID,
					DependentJobID:   externalProjectJob.ID,
					DependentProject: externalProjectSpec,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo).Once()
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return(jobDependenciesPairs, nil)

			projJobSpecRepoFac.On("New", externalProjectSpec).Return(projectJobSpecRepo).Once()
			projectJobSpecRepo.On("GetByIDs", ctx, []uuid.UUID{externalProjectJob.ID}).Return([]models.JobSpec{}, errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to resolve priority", func(t *testing.T) {
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

			jobDependenciesPairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpecsBase[0].ID,
					DependentJobID:   jobSpecsBase[1].ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return(jobDependenciesPairs, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return([]models.JobSpec{}, errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should fail when unable to deploy jobs", func(t *testing.T) {
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

			jobDependenciesPairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobSpecsBase[0].ID,
					DependentJobID:   jobSpecsBase[1].ID,
					DependentProject: projectSpec,
					Type:             models.JobSpecDependencyTypeIntra,
				},
			}

			projJobSpecRepoFac.On("New", projectSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("FetchJobDependencies", ctx, projectSpec.ID).Return(jobDependenciesPairs, nil)
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[0]).Return([]models.JobSpecHook{}).Once()
			dependencyResolver.On("FetchHookWithDependencies", jobSpecsAfterJobDependencyEnrich[1]).Return([]models.JobSpecHook{}).Once()

			priorityResolver.On("Resolve", ctx, jobSpecsAfterHookDependencyEnrich, nil).Return(jobSpecsAfterPriorityResolution, nil)

			batchScheduler.On("DeployJobs", ctx, namespaceSpec1, []models.JobSpec{jobSpecsAfterPriorityResolution[0]}, nil).Return(nil)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec2, []models.JobSpec{jobSpecsAfterPriorityResolution[1]}, nil).Return(errors.New(errorMsg))

			deployer := job.NewDeployer(dependencyResolver, priorityResolver, projJobSpecRepoFac, batchScheduler)
			err := deployer.Deploy(ctx, projectSpec, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
	})
}
