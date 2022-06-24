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
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			enrichedJobSpec := jobSpec
			enrichedJobSpec.ID = uuid.New()

			repo := new(mock.JobSpecRepository)
			repo.On("Save", ctx, jobSpec, "bigquery://project.dataset.table").Return(nil)
			repo.On("GetByName", ctx, jobSpec.Name).Return(enrichedJobSpec, nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, pluginService, nil, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.Nil(t, err)
		})

		t.Run("should not fail if dependency module is not found in plugin service", func(t *testing.T) {
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
			enrichedJobSpec := jobSpec
			enrichedJobSpec.ID = uuid.New()

			repo := new(mock.JobSpecRepository)

			repo.On("Save", ctx, jobSpec, "://").Return(nil)
			repo.On("GetByName", ctx, jobSpec.Name).Return(enrichedJobSpec, nil)
			// confirm with sandeep

			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(&models.GenerateDestinationResponse{}, service.ErrDependencyModNotFound)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, pluginService, nil, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
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
			repo.On("Save", ctx, jobSpec, "bigquery://project.dataset.table").Return(errors.New("unknown error"))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.NotNil(t, err)
		})
		t.Run("should fail if getting the updated jobspec failed", func(t *testing.T) {
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
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			errorMsg := "internal error"

			repo := new(mock.JobSpecRepository)
			repo.On("Save", ctx, jobSpec, "bigquery://project.dataset.table").Return(nil)
			repo.On("GetByName", ctx, jobSpec.Name).Return(models.JobSpec{}, errors.New(errorMsg))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, pluginService, nil, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.Contains(t, err.Error(), errorMsg)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		t.Run("should get jobSpec by the name", func(t *testing.T) {
			jobSpec := models.JobSpec{
				ID:      uuid.Must(uuid.NewUUID()),
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
			repo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
			_, err := svc.GetByName(ctx, jobSpec.Name, namespaceSpec)
			assert.Nil(t, err)
		})

		t.Run("should fail if repo fail", func(t *testing.T) {
			jobSpec := models.JobSpec{
				ID:      uuid.Must(uuid.NewUUID()),
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
			repo.On("GetByName", ctx, jobSpec.Name).Return(nil, errors.New("unknown error"))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.JobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
			_, err := svc.GetByName(ctx, jobSpec.Name, namespaceSpec)
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

			jobService := job.NewService(nil, batchScheduler, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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

			jobService := job.NewService(nil, batchScheduler, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, dependencyResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, priorityResolver, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
			err := svc.Sync(ctx, namespaceSpec, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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
		sampleResourceURN := "resource-a"

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
					Task:                models.JobSpecTask{},
					ResourceDestination: sampleResourceURN,
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("Delete", ctx, jobSpecsBase[0].ID).Return(nil)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("GetByResourceURN", ctx, sampleResourceURN).Return([]models.JobSource{}, nil)
			defer jobSourceRepo.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec, []string{jobSpecsBase[0].Name}, nil).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil,
				projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, jobSourceRepo)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.Nil(t, err)
		})

		t.Run("should return error if unable to get all jobs for checking static dependencies", func(t *testing.T) {
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

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			errorMsg := "internal error"
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil,
				projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, jobSourceRepo)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.Contains(t, err.Error(), errorMsg)
		})

		t.Run("should fail to delete a job spec if it is an inferred dependency of some other job", func(t *testing.T) {
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:                models.JobSpecTask{},
					ResourceDestination: sampleResourceURN,
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

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSource := models.JobSource{
				JobID:       jobSpecsBase[1].ID,
				ProjectID:   projSpec.ID,
				ResourceURN: sampleResourceURN,
			}
			jobSourceRepo.On("GetByResourceURN", ctx, jobSpecsBase[0].ResourceDestination).Return([]models.JobSource{jobSource}, nil)
			defer jobSourceRepo.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, jobSourceRepo)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.NotNil(t, err)
			assert.Equal(t, "cannot delete job test since it's dependency of other job", err.Error())
		})

		t.Run("should fail to delete a job spec if it is a static dependency of some other job", func(t *testing.T) {
			jobName1 := "test"
			jobSpecsBase := []models.JobSpec{
				{
					Version: 1,
					Name:    jobName1,
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:                models.JobSpecTask{},
					ResourceDestination: sampleResourceURN,
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
						jobName1: {},
					},
				},
			}

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("GetByResourceURN", ctx, jobSpecsBase[0].ResourceDestination).Return([]models.JobSource{}, nil)
			defer jobSourceRepo.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, jobSourceRepo)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.NotNil(t, err)
			assert.Equal(t, "cannot delete job test since it's dependency of other job", err.Error())
		})

		t.Run("should return error if unable to delete job spec", func(t *testing.T) {
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

			jobSpecRepo := new(mock.JobSpecRepository)
			errorMsg := "internal error"
			jobSpecRepo.On("Delete", ctx, jobSpecsBase[0].ID).Return(errors.New(errorMsg))
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			defer jobSpecRepoFac.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			defer projJobSpecRepoFac.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			jobSourceRepo.On("GetByResourceURN", ctx, jobSpecsBase[0].ResourceDestination).Return([]models.JobSource{}, nil)
			defer jobSourceRepo.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(jobSpecRepoFac, batchScheduler, nil, dumpAssets, depenResolver, nil,
				projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, jobSourceRepo)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.Contains(t, err.Error(), errorMsg)
		})
	})

	t.Run("Deploy", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}
		namespaceSpec := models.NamespaceSpec{
			ID:          uuid.Must(uuid.NewRandom()),
			Name:        "dev-team-1",
			ProjectSpec: projSpec,
		}
		createJobSpecDummy := func(N int) []models.JobSpec {
			jobSpecs := []models.JobSpec{}
			for i := 0; i < N; i++ {
				jobSpec := models.JobSpec{
					Version: 1,
					Name:    fmt.Sprintf("test-%d", i),
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					NamespaceSpec: namespaceSpec,
				}
				jobSpecs = append(jobSpecs, jobSpec)
			}
			return jobSpecs
		}
		errorMsg := "internal error"

		t.Run("should failed when unable to get namespace spec", func(t *testing.T) {
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, nil, nil, namespaceService, nil, nil, pluginService, nil, nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Contains(t, err.Error(), errorMsg)
		})
		t.Run("should failed when unable to get all jobs in the namespace when checking diff", func(t *testing.T) {
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			jobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, nil, nil, nil, nil, namespaceService, nil, nil, pluginService, nil, nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Equal(t, err.Error(), errorMsg)
		})
		t.Run("should not fail when one of job unable to be persisted", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			deployID := models.DeploymentID(uuid.New())

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)

			// 1 existing job is failed to be saved
			jobSpecRepo.On("Save", ctx, modifiedJobs[0], destination.URN()).Return(errors.New(errorMsg))

			// the rest of the jobs are succeeded to be persisted
			for i := 1; i < len(modifiedJobs)-1; i++ {
				jobSpecRepo.On("Save", ctx, modifiedJobs[i], destination.URN()).Return(nil)
				jobSpecRepo.On("GetByName", ctx, modifiedJobs[i].Name).Return(modifiedJobs[i], nil)
			}

			// 1 new job is failed to be saved
			jobSpecRepo.On("Save", ctx, modifiedJobs[len(modifiedJobs)-1], destination.URN()).Return(errors.New(errorMsg))

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(requestedJobSpecs, nil)

			for _, jobSpec := range deletedJobs {
				jobSourceRepo.On("GetByResourceURN", ctx, deletedJobs[1].ResourceDestination).Return([]models.JobSource{}, nil).Once()
				jobSpecRepo.On("Delete", ctx, jobSpec.ID).Return(nil)
			}

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i := 1; i < len(modifiedJobs)-1; i++ {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, modifiedJobs[i].ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, nil, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Nil(t, err)
		})
		t.Run("should failed when unable to get all jobs in the project to check static dependencies", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				jobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				jobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, nil, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Contains(t, err.Error(), errorMsg)
		})
		t.Run("should not failed when one of the job is not deletable", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			deployID := models.DeploymentID(uuid.New())

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				jobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				jobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			projectJobSpecRepo.On("GetAll", ctx).Return(requestedJobSpecs, nil)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			// one job with failing case
			deletedJobs[0].ResourceDestination = "resource-b"
			jobSources := []models.JobSource{
				{
					JobID:       uuid.New(),
					ProjectID:   projSpec.ID,
					ResourceURN: deletedJobs[0].ResourceDestination,
				},
			}
			jobSourceRepo.On("GetByResourceURN", ctx, deletedJobs[0].ResourceDestination).Return(jobSources, nil)

			// rest of the jobs with success state
			for i := 1; i < len(deletedJobs); i++ {
				jobSourceRepo.On("GetByResourceURN", ctx, deletedJobs[i].ResourceDestination).Return([]models.JobSource{}, nil)
				jobSpecRepo.On("Delete", ctx, deletedJobs[i].ID).Return(nil)
			}

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, nil, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Nil(t, err)
		})
		t.Run("should not failed when one of the job is failed to be deleted", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			deployID := models.DeploymentID(uuid.New())

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				jobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				jobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			projectJobSpecRepo.On("GetAll", ctx).Return(requestedJobSpecs, nil)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			// one job with failing case
			deletedJobs[0].ResourceDestination = "resource-b"
			jobSourceRepo.On("GetByResourceURN", ctx, deletedJobs[0].ResourceDestination).Return([]models.JobSource{}, nil)
			jobSpecRepo.On("Delete", ctx, deletedJobs[0].ID).Return(errors.New(errorMsg))

			// rest of the jobs with success state
			for i := 1; i < len(deletedJobs); i++ {
				jobSourceRepo.On("GetByResourceURN", ctx, deletedJobs[i].ResourceDestination).Return([]models.JobSource{}, nil)
				jobSpecRepo.On("Delete", ctx, deletedJobs[i].ID).Return(nil)
			}

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, nil, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Nil(t, err)
		})

		t.Run("should failed when unable to create deployment request to deploy manager", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				jobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				jobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			projectJobSpecRepo.On("GetAll", ctx).Return(requestedJobSpecs, nil)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			for _, deletedJob := range deletedJobs {
				jobSourceRepo.On("GetByResourceURN", ctx, deletedJob.ResourceDestination).Return([]models.JobSource{}, nil)
				jobSpecRepo.On("Delete", ctx, deletedJob.ID).Return(nil)
			}

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(models.DeploymentID{}, errors.New(errorMsg))

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, nil, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Equal(t, err.Error(), errorMsg)
		})
		t.Run("should deploy successfully", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			deployID := models.DeploymentID(uuid.New())

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSpecRepoFac := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				jobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				jobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			projectJobSpecRepo.On("GetAll", ctx).Return(requestedJobSpecs, nil)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			for _, jobSpec := range deletedJobs {
				jobSourceRepo.On("GetByResourceURN", ctx, jobSpec.ResourceDestination).Return([]models.JobSource{}, nil)
				jobSpecRepo.On("Delete", ctx, jobSpec.ID).Return(nil)
			}

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, nil, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, nil)
			assert.Nil(t, err)
		})
	})

	t.Run("GetByDestination", func(t *testing.T) {
		t.Run("should return job spec given a destination", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				Name:        "namespace",
				ProjectSpec: projSpec,
			}
			destination := "resource-urn"
			jobSpec1 := models.JobSpec{Name: "dag1-no-deps", Dependencies: map[string]models.JobSpecDependency{}, NamespaceSpec: namespaceSpec}

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)
			projectJobSpecRepo.On("GetByDestination", ctx, destination).Return(jobSpec1, nil)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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
			projectJobSpecRepo.On("GetByDestination", ctx, destination).Return(models.JobSpec{}, errors.New(errorMsg))

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)
			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
			jobSpecsResult, err := svc.GetByDestination(ctx, projSpec, destination)
			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.JobSpec{}, jobSpecsResult)
		})
	})
	t.Run("GetByFilter", func(t *testing.T) {
		t.Run("should return job spec given a filter", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-1",
				ProjectSpec: projSpec,
			}
			jobSpec1 := models.JobSpec{Name: "dag1-no-deps", Dependencies: map[string]models.JobSpecDependency{}, NamespaceSpec: namespaceSpec}
			destination := "resource-urn"
			jobSpecs := []models.JobSpec{jobSpec1}

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			interProjectJobSpecRepo := new(mock.InterProjectJobSpecRepository)
			defer interProjectJobSpecRepo.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecs, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			interProjectJobSpecRepo.On("GetJobByName", ctx, jobSpec1.GetName()).Return(models.JobSpecs{jobSpec1}, nil)
			interProjectJobSpecRepo.On("GetJobByResourceDestination", ctx, destination).Return(jobSpec1, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, projectService, nil, nil, interProjectJobSpecRepo, nil)
			jobSpecsResult, err := svc.GetByFilter(ctx, models.JobSpecFilter{JobName: jobSpec1.Name})
			assert.Nil(t, err)
			assert.Equal(t, jobSpecs, jobSpecsResult)

			jobSpecsResult1, err := svc.GetByFilter(ctx, models.JobSpecFilter{ResourceDestination: destination})
			assert.Nil(t, err)
			assert.Equal(t, jobSpecs, jobSpecsResult1)
			//
			jobSpecsResult2, err := svc.GetByFilter(ctx, models.JobSpecFilter{ProjectName: jobSpec1.NamespaceSpec.ProjectSpec.Name})
			assert.Nil(t, err)
			assert.Equal(t, jobSpecs, jobSpecsResult2)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, dumpAssets, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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
		deployID := models.DeploymentID(uuid.New())
		errorMsg := "internal error"
		jobID := uuid.New()
		resourceURNs := []string{"resource-a"}

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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			jobSpecsBase := []models.JobSpec{
				{
					ID:      jobID,
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
			}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			jobSpecsBase := []models.JobSpec{
				{
					ID:      jobID,
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
			}
			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			jobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			jobSpecsBase := []models.JobSpec{
				{
					ID:      jobID,
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
			}
			jobNames := []string{jobSpecsBase[0].Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetByName", ctx, jobNames[0]).Return(jobSpecsBase[0], namespaceSpec, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			svc := job.NewService(nil, batchScheduler, nil, dumpAssets, dependencyResolver,
				priorityResolver, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(models.ProjectSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, nil, nil, nil)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, nil, nil, nil)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployManager, nil, nil, nil)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			jobSpecRepoFac.On("New", namespaceSpec).Return(jobSpecRepo)
			jobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(jobSpecRepoFac, nil, nil, dumpAssets, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployManager, nil, nil, nil)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

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
				nil, projectJobSpecRepoFac, nil, namespaceService, projectService, deployManager, nil, nil, nil)
			err := svc.Refresh(ctx, projSpec.Name, nil, jobNames, nil)

			assert.Equal(t, fmt.Sprintf("failed to retrieve job: %s", errorMsg), err.Error())
		})
		t.Run("should not failed refresh when one of the generating dependency call failed", func(t *testing.T) {
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			jobSpecsBase := []models.JobSpec{
				{
					ID:      jobID,
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
				{
					ID:      uuid.New(),
					Version: 1,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
			}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil).Once()
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil).Once()

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[1], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, errors.New(errorMsg))

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			jobSpecsBase := []models.JobSpec{
				{
					ID:      jobID,
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
				{
					ID:      uuid.New(),
					Version: 1,
					Name:    "test-2",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
			}
			resourceURNs2 := []string{"resource-b"}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[1], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs2}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobSpecsBase[1].ID, resourceURNs2).Return(errors.New(errorMsg))

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Nil(t, err)
		})
		t.Run("should failed when unable to create deployment request to deploy manager", func(t *testing.T) {
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

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			jobSpecsBase := []models.JobSpec{
				{
					ID:      jobID,
					Version: 1,
					Name:    "test",
					Owner:   "optimus",
					Schedule: models.JobSpecSchedule{
						StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
						Interval:  "@daily",
					},
					Task:          models.JobSpecTask{},
					NamespaceSpec: namespaceSpec,
				},
			}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(models.DeploymentID{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)
			err := svc.Refresh(ctx, projSpec.Name, nil, nil, nil)

			assert.Contains(t, err.Error(), errorMsg)
		})
	})

	t.Run("GetDeployment", func(t *testing.T) {
		projSpec := models.ProjectSpec{
			Name: "proj",
		}
		t.Run("should able to get job deployment successfully", func(t *testing.T) {
			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			deployID := models.DeploymentID(uuid.New())
			expectedDeployment := models.JobDeployment{
				ID:      deployID,
				Project: projSpec,
				Status:  models.JobDeploymentStatusInProgress,
			}

			deployManager.On("GetStatus", ctx, deployID).Return(expectedDeployment, nil)

			svc := job.NewService(nil, nil, nil, dumpAssets, nil,
				nil, nil, nil, nil, nil, deployManager, nil, nil, nil)
			deployment, err := svc.GetDeployment(ctx, deployID)

			assert.Nil(t, err)
			assert.Equal(t, expectedDeployment, deployment)
		})
		t.Run("should failed when unable to get job deployment", func(t *testing.T) {
			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			deployID := models.DeploymentID(uuid.New())
			errorMsg := "internal error"

			deployManager.On("GetStatus", ctx, deployID).Return(models.JobDeployment{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dumpAssets, nil,
				nil, nil, nil, nil, nil, deployManager, nil, nil, nil)
			deployment, err := svc.GetDeployment(ctx, deployID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.JobDeployment{}, deployment)
		})
	})
}
