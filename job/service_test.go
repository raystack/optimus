package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
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

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, nil)
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

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, currentSpec, namespaceSpec, true).
				Return(&models.GenerateDependenciesResponse{}, service.ErrDependencyModNotFound)
			defer pluginService.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("VerifyJob", ctx, namespaceSpec, currentSpec).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			jobService := job.NewService(nil, batchScheduler, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService)
			err := jobService.Check(ctx, namespaceSpec, []models.JobSpec{currentSpec}, nil)
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

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDependencies", ctx, currentSpec, namespaceSpec, true).
				Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("VerifyJob", ctx, namespaceSpec, currentSpec).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			jobService := job.NewService(nil, batchScheduler, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService)
			err := jobService.Check(ctx, namespaceSpec, []models.JobSpec{currentSpec}, nil)
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
			jobSpecsAfterDependencyResolved := []models.JobSpec{
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
			dependencyResolver := new(mock.DependencyResolver)
			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDependencyResolved[0], nil)
			defer dependencyResolver.AssertExpectations(t)

			// resolve priority
			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", ctx, jobSpecsAfterDependencyResolved, nil).Return(jobSpecsAfterPriorityResolve, nil)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeployJobs", ctx, namespaceSpec, jobSpecsAfterPriorityResolve, nil).Return(nil)
			batchScheduler.On("ListJobs", ctx, namespaceSpec, models.SchedulerListOptions{OnlyName: true}).Return(jobs, nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, dependencyResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			pluginService := new(mock.DependencyResolverPluginService)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).
				Return(&models.GenerateDestinationResponse{
					Destination: "project.dataset.table",
					Type:        "bq",
				}, nil)
			pluginService.On("GenerateDependencies", ctx, jobSpec, namespaceSpec, false).
				Return(&models.GenerateDependenciesResponse{
					Dependencies: []string{"bq://project.dataset.table"},
				}, nil)
			defer pluginService.AssertExpectations(t)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
			jobSpecsResult, err := svc.GetByDestination(ctx, projSpec, destination)
			assert.Nil(t, err)
			assert.Equal(t, jobSpec1, jobSpecsResult)
		})
		t.Run("should return error when unable to fetch job spec using destination", func(t *testing.T) {
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

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil)
			jobSpecsResult, err := svc.GetDownstream(ctx, projSpec, destination)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, jobSpecsResult)
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}
		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projSpec,
		}
		errorMsg := "internal error"

		t.Run("should successfully refresh job specs for the whole project", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

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
			jobSpecsAfterDependencyResolved := []models.JobSpec{
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

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDependencyResolved[0], nil)
			dependencyResolver.On("Persist", ctx, jobSpecsBase[0]).Return(nil)

			deployer.On("Deploy", ctx, projSpec, nil).Return(nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Nil(t, err)
		})
		t.Run("should successfully refresh job specs for a namespace", func(t *testing.T) {
			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

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
			jobSpecsAfterDependencyResolved := []models.JobSpec{
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
			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			jobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDependencyResolved[0], nil)
			dependencyResolver.On("Persist", ctx, jobSpecsBase[0]).Return(nil)

			deployer.On("Deploy", ctx, projSpec, nil).Return(nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, namespaceNames, nil, nil)

			assert.Nil(t, err)
		})
		t.Run("should successfully refresh job specs for the selected jobs", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

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
			jobSpecsAfterDependencyResolved := []models.JobSpec{
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
			jobNames := []string{jobSpecsBase[0].Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetByName", ctx, jobNames[0]).Return(jobSpecsBase[0], namespaceSpec, nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDependencyResolved[0], nil)
			dependencyResolver.On("Persist", ctx, jobSpecsBase[0]).Return(nil)

			deployer.On("Deploy", ctx, projSpec, nil).Return(nil)

			svc := job.NewService(nil, batchScheduler, nil, dumpAssets, dependencyResolver,
				priorityResolver, projJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, jobNames, nil)

			assert.Nil(t, err)
		})
		t.Run("should failed when unable to get project spec", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(models.ProjectSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should failed when unable to fetch job specs when refreshing whole project", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Equal(t, fmt.Sprintf("failed to retrieve jobs: %s", errorMsg), err.Error())
		})
		t.Run("should failed when unable to get namespaceSpec when refreshing a namespace", func(t *testing.T) {
			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, namespaceNames, nil, nil)

			assert.Equal(t, errorMsg, err.Error())
		})
		t.Run("should failed when unable to fetch job specs when refreshing a namespace", func(t *testing.T) {
			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			jobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, namespaceNames, nil, nil)

			assert.Equal(t, fmt.Sprintf("failed to retrieve jobs: %s", errorMsg), err.Error())
		})
		t.Run("should failed when unable to fetch job specs when refreshing selected jobs", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projectJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

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
			jobNames := []string{jobSpecsBase[0].Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projectJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetByName", ctx, jobSpecsBase[0].Name).Return(models.JobSpec{}, models.NamespaceSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projectJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, jobNames, nil)

			assert.Equal(t, fmt.Sprintf("failed to retrieve job: %s", errorMsg), err.Error())
		})
		t.Run("should not failed refresh when one of dependency resolution failed", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

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
			jobSpecsAfterDependencyResolved := []models.JobSpec{
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

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDependencyResolved[0], nil)
			dependencyResolver.On("Persist", ctx, jobSpecsAfterDependencyResolved[0]).Return(nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[1], nil).Return(models.JobSpec{}, errors.New(errorMsg))

			deployer.On("Deploy", ctx, projSpec, nil).Return(nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Nil(t, err)
		})
		t.Run("should not failed refresh when one of persisting dependency process failed", func(t *testing.T) {
			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			deployer := new(mock.Deployer)
			defer deployer.AssertExpectations(t)

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
			jobSpecsAfterDependencyResolved := []models.JobSpec{
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

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[0], nil).Return(jobSpecsAfterDependencyResolved[0], nil)
			dependencyResolver.On("Persist", ctx, jobSpecsAfterDependencyResolved[0]).Return(nil)

			dependencyResolver.On("Resolve", ctx, projSpec, jobSpecsBase[1], nil).Return(jobSpecsAfterDependencyResolved[1], nil)
			dependencyResolver.On("Persist", ctx, jobSpecsAfterDependencyResolved[1]).Return(errors.New(errorMsg))

			deployer.On("Deploy", ctx, projSpec, nil).Return(nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployer, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Nil(t, err)
		})
	})
}
