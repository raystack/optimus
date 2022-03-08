package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestService(t *testing.T) {
	ctx := context.Background()

	var dumpAssets job.AssetCompiler = func(jobSpec models.JobSpec, _ time.Time) (models.JobAssets, error) {
		return jobSpec.Assets, nil
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("should create a new JobSpec and store in repository", func(t *testing.T) {
			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
			}
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-1",
				ProjectSpec: projSpec,
			}

			repo := new(mock.JobSpecRepository)
			repo.On("Save", ctx, jobSpec).Return(nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil)
			err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.Nil(t, err)
		})

		t.Run("should fail if saving to repo fails", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-1",
				ProjectSpec: projSpec,
			}
			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
			}

			repo := new(mock.JobSpecRepository)
			repo.On("Save", ctx, jobSpec).Return(errors.New("unknown error"))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, nil, nil)
			err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.NotNil(t, err)
		})
	})

	t.Run("Check", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}
		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projSpec,
		}
		t.Run("should skip checking for dependencies for task that doesn't support this mod", func(t *testing.T) {
			currentSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{},
				},
				Dependencies: map[string]models.JobSpecDependency{},
			}

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("VerifyJob", ctx, namespaceSpec, currentSpec).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			service := job.NewService(nil, batchScheduler, nil, dumpAssets, nil, nil, nil, nil)
			err := service.Check(ctx, namespaceSpec, []models.JobSpec{currentSpec}, nil)
			assert.Nil(t, err)
		})
		t.Run("should check for successful dependency resolution for task that does support this mod", func(t *testing.T) {
			depMode := new(mock.DependencyResolverMod)
			defer depMode.AssertExpectations(t)
			currentSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Task: models.JobSpecTask{
					Unit: &models.Plugin{DependencyMod: depMode},
				},
				Dependencies: map[string]models.JobSpecDependency{},
			}
			depMode.On("GenerateDependencies", context.Background(), models.GenerateDependenciesRequest{
				Config:  models.PluginConfigs{}.FromJobSpec(currentSpec.Task.Config),
				Assets:  models.PluginAssets{}.FromJobSpec(currentSpec.Assets),
				Project: namespaceSpec.ProjectSpec,
				PluginOptions: models.PluginOptions{
					DryRun: true,
				},
			}).Return(&models.GenerateDependenciesResponse{}, nil)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("VerifyJob", ctx, namespaceSpec, currentSpec).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			service := job.NewService(nil, batchScheduler, nil, dumpAssets, nil, nil, nil, nil)
			err := service.Check(ctx, namespaceSpec, []models.JobSpec{currentSpec}, nil)
			assert.Nil(t, err)
		})
	})

	t.Run("Sync", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projSpec,
		}

		t.Run("should successfully store job specs for the requested project", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterPriorityResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
				},
			}

			jobs := []models.Job{
				{
					Name: "test",
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name: {jobSpecsBase[0].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDepenResolve[0], nil)
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", ctx, jobSpecsAfterDepenResolve, nil).Return(jobSpecsAfterPriorityResolve, nil)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec, jobSpecsAfterPriorityResolve, nil).Return(nil)
			batchScheduler.On("ListJobs", ctx, namespaceSpec, models.SchedulerListOptions{OnlyName: true}).Return(jobs, nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.Nil(t, err)
		})
		t.Run("should ignore the failure of dependency resolution of different namespaces", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "test-but-for-different-namespace",
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterPriorityResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
				},
			}

			jobs := []models.Job{
				{
					Name: "test",
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name:  {jobSpecsBase[0].Name},
				"not-our-namespace": {jobSpecsBase[1].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDepenResolve[0], nil)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[1], nil).Return(models.JobSpec{}, errors.New("failed to resolve"))
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", ctx, jobSpecsAfterDepenResolve, nil).Return(jobSpecsAfterPriorityResolve, nil)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec, jobSpecsAfterPriorityResolve, nil).Return(nil)
			batchScheduler.On("ListJobs", ctx, namespaceSpec, models.SchedulerListOptions{OnlyName: true}).Return(jobs, nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.Nil(t, err)
		})
		t.Run("should ignore the failure of dependency resolution of different namespaces but not current one", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "test-but-for-different-namespace",
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name:  {jobSpecsBase[0].Name},
				"not-our-namespace": {jobSpecsBase[1].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(models.JobSpec{}, errors.New("failed to resolve 1"))
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[1], nil).Return(models.JobSpec{}, errors.New("failed to resolve 2"))
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.NotNil(t, err)
		})
		t.Run("should ignore the failure of dependency resolution of different namespaces but not any other error", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "test-but-for-different-namespace",
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(nil, errors.New("some error"))
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.NotNil(t, err)
		})
		t.Run("should delete job specs from target store if there are existing specs that are no longer present in job specs", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterPriorityResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
				},
			}

			jobs := []models.Job{
				{
					Name: "test",
				},
				{
					Name: "test2",
				},
			}

			// used to store raw job specs
			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name: {jobSpecsBase[0].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDepenResolve[0], nil)
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", ctx, jobSpecsAfterDepenResolve, nil).Return(jobSpecsAfterPriorityResolve, nil)
			defer priorityResolver.AssertExpectations(t)

			// fetch currently stored
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec, jobSpecsAfterPriorityResolve, nil).Return(nil)
			batchScheduler.On("ListJobs", ctx, namespaceSpec, models.SchedulerListOptions{OnlyName: true}).Return(jobs, nil)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec, []string{jobs[1].Name}, nil).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.Nil(t, err)
		})

		t.Run("should batch dependency resolution errors if any for all jobs", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}

			// used to store raw job specs
			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name: {jobSpecsBase[1].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(models.JobSpec{}, errors.New("error test"))
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[1], nil).Return(models.JobSpec{},
				errors.New("error test-2"))
			defer depenResolver.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "error test")
			assert.Contains(t, err.Error(), "error test-2")
		})

		t.Run("should successfully publish metadata for all job specs", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterPriorityResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{
						Priority: 10000,
					},
				},
			}

			jobs := []models.Job{
				{
					Name: "test",
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name: {jobSpecsBase[0].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDepenResolve[0], nil)
			defer depenResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", ctx, jobSpecsAfterDepenResolve, nil).Return(jobSpecsAfterPriorityResolve, nil)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec, jobSpecsAfterPriorityResolve, nil).Return(nil)
			batchScheduler.On("ListJobs", ctx, namespaceSpec, models.SchedulerListOptions{OnlyName: true}).Return(jobs, nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
			assert.Nil(t, err)
		})
	})

	t.Run("KeepOnly", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projSpec,
		}

		t.Run("should keep only provided specs and delete rest", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test-1",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}

			toKeep := []models.JobSpec{
				{
					Version: 1,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}

			// used to store raw job specs
			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// fetch currently stored
			jobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			// delete unwanted
			jobSpecRepo.On("Delete", ctx, jobSpecsBase[0].Name).Return(nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil)
			err := svc.KeepOnly(ctx, namespaceSpec, toKeep, nil)
			assert.Nil(t, err)
		})
	})

	t.Run("GetTaskDependencies", func(t *testing.T) {
		projectSpec := models.ProjectSpec{
			Name: "proj",
		}
		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projectSpec,
		}
		t.Run("should successfully generate destination and dependencies for job task", func(t *testing.T) {
			execUnit1 := new(mock.DependencyResolverMod)
			defer execUnit1.AssertExpectations(t)
			jobSpec := models.JobSpec{
				Version: 1,
				Name:    "test",
				Task: models.JobSpecTask{
					Unit: &models.Plugin{
						Base:          execUnit1,
						DependencyMod: execUnit1,
					},
					Config: models.JobSpecConfigs{
						{
							Name:  "do",
							Value: "this",
						},
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					},
				),
			}

			execUnit1.On("GenerateDestination", ctx, models.GenerateDestinationRequest{
				Config:  models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				Project: projectSpec,
			}).Return(&models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        "bq",
			}, nil)
			execUnit1.On("GenerateDependencies", ctx, models.GenerateDependenciesRequest{
				Config:  models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				Project: projectSpec,
			}).Return(&models.GenerateDependenciesResponse{
				Dependencies: []string{"bq://project.dataset.table"},
			}, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, nil, nil)
			dest, depen, err := svc.GetTaskDependencies(ctx, namespaceSpec, jobSpec)
			assert.Nil(t, err)
			assert.Equal(t, models.JobSpecTaskDestination{
				Destination: "project.dataset.table",
				Type:        "bq",
			}, dest)
			assert.Equal(t, models.JobSpecTaskDependencies{
				"bq://project.dataset.table",
			}, depen)
		})
	})
	t.Run("Delete", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}

		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projSpec,
		}

		t.Run("should successfully delete a job spec", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}

			jobs := []models.Job{
				{
					Name: "test",
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("Delete", ctx, "test").Return(nil)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name: {jobSpecsBase[0].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDepenResolve[0], nil)
			defer depenResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec, []string{jobs[0].Name}, nil).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.Nil(t, err)
		})

		t.Run("should fail to delete a job spec if it is dependency of some other job", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "downstream-test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
			}
			jobSpecsAfterDepenResolve := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
				},
				{
					Version: 1,
					Name:    "downstream-test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task: models.JobSpecTask{},
					Dependencies: map[string]models.JobSpecDependency{
						// set the test job spec as dependency of this job
						jobSpecsBase[0].Name: {Job: &jobSpecsBase[0], Project: &projSpec, Type: models.JobSpecDependencyTypeInter},
					},
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				namespaceSpec.Name: {jobSpecsBase[1].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDepenResolve[0], nil)
			depenResolver.On("Resolve", ctx, projSpec, jobSpecsBase[1], nil).Return(jobSpecsAfterDepenResolve[1], nil)
			defer depenResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.NotNil(t, err)
			assert.Equal(t, "cannot delete job test since it's dependency of job downstream-test", err.Error())
		})
	})

	t.Run("GetByDestination", func(t *testing.T) {
		t.Run("should return job spec given a destination", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			destination := "resource-urn"
			jobSpec1 := models.JobSpec{Name: "dag1-no-deps", Dependencies: map[string]models.JobSpecDependency{}}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)
			projectJobSpecRepo.On("GetByDestination", ctx, destination).Return([]store.ProjectJobPair{
				{
					Project: projSpec,
					Job:     jobSpec1,
				},
			}, nil)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil)
			jobSpecsResult, err := svc.GetByDestination(ctx, projSpec, destination)
			assert.Nil(t, err)
			assert.Equal(t, jobSpec1, jobSpecsResult)
		})
		t.Run("should return error when unable to fetch jobspec using destination", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			destination := "resource-urn"

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)
			errorMsg := "unable to fetch jobspec"
			projectJobSpecRepo.On("GetByDestination", ctx, destination).Return(nil, errors.New(errorMsg))

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil)
			jobSpecsResult, err := svc.GetByDestination(ctx, projSpec, destination)
			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.JobSpec{}, jobSpecsResult)
		})
	})
	t.Run("GetDownstream", func(t *testing.T) {
		t.Run("should return downstream job specs", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			jobSpecsMap := make(map[string]models.JobSpec)
			jobSpec1 := models.JobSpec{Name: "dag1-no-deps", Dependencies: map[string]models.JobSpecDependency{}}
			jobSpecsMap[jobSpec1.GetName()] = jobSpec1
			jobSpec2 := models.JobSpec{Name: "dag2-deps-on-dag1", Dependencies: getDependencyObject(jobSpecsMap, jobSpec1.GetName())}
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecs, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				"a": {jobSpecs[0].Name},
				"b": {jobSpecs[1].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)
			depenResolver.On("Resolve", ctx, projSpec, jobSpec1, nil).Return(jobSpec1, nil)
			depenResolver.On("Resolve", ctx, projSpec, jobSpec2, nil).Return(jobSpec2, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil)
			jobSpecsResult, err := svc.GetDownstream(ctx, projSpec, jobSpec1.Name)
			assert.Nil(t, err)
			assert.Equal(t, []models.JobSpec{jobSpec2}, jobSpecsResult)
		})
		t.Run("should return error when unable to get all job specs to resolve dependency", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			destination := "resource-urn"
			jobSpecsMap := make(map[string]models.JobSpec)
			jobSpec1 := models.JobSpec{Name: "dag1-no-deps", Dependencies: map[string]models.JobSpecDependency{}}
			jobSpecsMap[jobSpec1.GetName()] = jobSpec1
			jobSpec2 := models.JobSpec{Name: "dag2-deps-on-dag1", Dependencies: getDependencyObject(jobSpecsMap, jobSpec1.GetName())}
			jobSpecsMap[jobSpec2.GetName()] = jobSpec2

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)
			errorMsg := "unable to get all job specs of a project"
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil)
			jobSpecsResult, err := svc.GetDownstream(ctx, projSpec, destination)
			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, jobSpecsResult)
		})
		t.Run("should return error when unable to resolve dependency", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			destination := "resource-urn"
			jobSpecsMap := make(map[string]models.JobSpec)
			jobSpec1 := models.JobSpec{Name: "dag1-no-deps", Dependencies: map[string]models.JobSpecDependency{}}
			jobSpecsMap[jobSpec1.GetName()] = jobSpec1
			jobSpec2 := models.JobSpec{Name: "dag2-deps-on-dag1", Dependencies: getDependencyObject(jobSpecsMap, jobSpec1.GetName())}
			jobSpecsMap[jobSpec2.GetName()] = jobSpec2
			jobSpecs := []models.JobSpec{jobSpec1, jobSpec2}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecs, nil)
			projectJobSpecRepo.On("GetJobNamespaces", ctx).Return(map[string][]string{
				"ns": {jobSpecs[0].Name, jobSpecs[1].Name},
			}, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)
			errorMsg := "unable to resolve dependency"
			depenResolver.On("Resolve", ctx, projSpec, jobSpec1, nil).Return(models.JobSpec{}, errors.New(errorMsg))
			depenResolver.On("Resolve", ctx, projSpec, jobSpec2, nil).Return(models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil)
			jobSpecsResult, err := svc.GetDownstream(ctx, projSpec, destination)
			assert := assert.New(t)
			assert.Contains(err.Error(), errorMsg)
			assert.Nil(jobSpecsResult)
		})
	})
}
