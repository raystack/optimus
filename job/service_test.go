package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

func TestService(t *testing.T) {
	ctx := context.Background()

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

			repo := new(mock.NamespaceJobSpecRepository)
			repo.On("Save", ctx, jobSpec, "bigquery://project.dataset.table").Return(nil)
			repo.On("GetByName", ctx, jobSpec.Name).Return(enrichedJobSpec, nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.NamespaceJobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			svc := job.NewService(repoFac, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, pluginService, nil, nil)
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

			repo := new(mock.NamespaceJobSpecRepository)

			repo.On("Save", ctx, jobSpec, "://").Return(nil)
			repo.On("GetByName", ctx, jobSpec.Name).Return(enrichedJobSpec, nil)
			// confirm with sandeep

			defer repo.AssertExpectations(t)

			repoFac := new(mock.NamespaceJobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(&models.GenerateDestinationResponse{}, service.ErrDependencyModNotFound)

			svc := job.NewService(repoFac, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, pluginService, nil, nil)
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

			repo := new(mock.NamespaceJobSpecRepository)
			repo.On("Save", ctx, jobSpec, "bigquery://project.dataset.table").Return(errors.New("unknown error"))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.NamespaceJobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			svc := job.NewService(repoFac, nil, nil, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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

			repo := new(mock.NamespaceJobSpecRepository)
			repo.On("Save", ctx, jobSpec, "bigquery://project.dataset.table").Return(nil)
			repo.On("GetByName", ctx, jobSpec.Name).Return(models.JobSpec{}, errors.New(errorMsg))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.NamespaceJobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			svc := job.NewService(repoFac, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, pluginService, nil, nil)
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
			repo := new(mock.NamespaceJobSpecRepository)
			repo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			defer repo.AssertExpectations(t)

			repoFac := new(mock.NamespaceJobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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
			repo := new(mock.NamespaceJobSpecRepository)
			repo.On("GetByName", ctx, jobSpec.Name).Return(nil, errors.New("unknown error"))
			defer repo.AssertExpectations(t)

			repoFac := new(mock.NamespaceJobSpecRepoFactory)
			repoFac.On("New", namespaceSpec).Return(repo)
			defer repoFac.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			svc := job.NewService(repoFac, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			logWriter := new(mock.LogWriter)
			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			defer logWriter.AssertExpectations(t)

			jobService := job.NewService(nil, batchScheduler, nil, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
			err := jobService.Check(ctx, namespaceSpec, []models.JobSpec{currentSpec}, logWriter)
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

			logWriter := new(mock.LogWriter)
			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			defer logWriter.AssertExpectations(t)

			jobService := job.NewService(nil, batchScheduler, nil, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
			err := jobService.Check(ctx, namespaceSpec, []models.JobSpec{currentSpec}, logWriter)
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

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			namespaceJobSpecRepo.On("Delete", ctx, jobSpecsBase[0].ID).Return(nil)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("GetDependentJobs", ctx, &jobSpecsBase[0]).Return([]models.JobSpec{}, nil)
			defer jobSpecRepo.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec, []string{jobSpecsBase[0].Name}, nil).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(namespaceJobSpecRepoFac, batchScheduler, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, jobSpecRepo, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.Nil(t, err)
		})

		t.Run("should fail to delete a job spec if it is a dependency of some other job", func(t *testing.T) {
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("GetDependentJobs", ctx, &jobSpecsBase[0]).Return([]models.JobSpec{jobSpecsBase[1]}, nil)
			defer jobSpecRepo.AssertExpectations(t)

			// resolve dependencies
			depenResolver := new(mock.DependencyResolver)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(namespaceJobSpecRepoFac, batchScheduler, nil, depenResolver, nil,
				nil, nil, nil, nil, nil, nil, jobSpecRepo, nil)
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			errorMsg := "internal error"
			namespaceJobSpecRepo.On("Delete", ctx, jobSpecsBase[0].ID).Return(errors.New(errorMsg))
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			jobSpecRepo.On("GetDependentJobs", ctx, &jobSpecsBase[0]).Return([]models.JobSpec{}, nil)
			defer jobSpecRepo.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(namespaceJobSpecRepoFac, batchScheduler, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, jobSpecRepo, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])
			assert.Contains(t, err.Error(), errorMsg)
		})
	})

	t.Run("Deploy", func(t *testing.T) {
		// TODO: remove it once job deploy is merged
		t.Skip()
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, namespaceService, nil, nil, pluginService, nil, nil)

			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
			assert.Contains(t, err.Error(), errorMsg)
		})
		t.Run("should failed when unable to get all jobs in the namespace when checking diff", func(t *testing.T) {
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)
			namespaceJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, nil, nil, nil, nil, namespaceService, nil, nil, pluginService, nil, nil)

			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
			assert.Equal(t, err.Error(), errorMsg)
		})
		t.Run("should not fail when two jobs unable to be persisted", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			for i := range existingJobSpecs {
				existingJobSpecs[i].ID = uuid.New()
			}
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs[0:1]
			modifiedJobs[0].ID = existingJobSpecs[9].ID
			newRequestedJobs := requestedJobSpecs[1:]
			createAndModifiedJobs := append(newRequestedJobs, modifiedJobs...)
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			deployID := models.DeploymentID(uuid.New())

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)

			// 1 new jobs is failed to be saved
			namespaceJobSpecRepo.On("Save", ctx, createAndModifiedJobs[0], destination.URN()).Return(errors.New(errorMsg))
			// 1 modified job is failed to be saved
			namespaceJobSpecRepo.On("Save", ctx, createAndModifiedJobs[len(createAndModifiedJobs)-1], destination.URN()).Return(errors.New(errorMsg))

			// the rest of the jobs are succeeded to be persisted
			for i := 1; i < len(createAndModifiedJobs)-1; i++ {
				namespaceJobSpecRepo.On("Save", ctx, createAndModifiedJobs[i], destination.URN()).Return(nil)
				namespaceJobSpecRepo.On("GetByName", ctx, createAndModifiedJobs[i].Name).Return(createAndModifiedJobs[i], nil)
			}

			for _, jobSpec := range deletedJobs {
				spec := jobSpec
				jobSpecRepo.On("GetDependentJobs", ctx, &spec).Return([]models.JobSpec{}, nil).Once()
				namespaceJobSpecRepo.On("Delete", ctx, jobSpec.ID).Return(nil)
			}

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)

			for _, jobSpec := range createAndModifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i := 1; i < len(createAndModifiedJobs)-1; i++ {
				pluginService.On("GenerateDependencies", ctx, createAndModifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, createAndModifiedJobs[i].ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, depenResolver, nil, projJobSpecRepoFac,
				nil, namespaceService, nil, deployManager, pluginService, jobSpecRepo, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
			assert.Nil(t, err)
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				namespaceJobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				namespaceJobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			// one job with failing case
			jobSpecRepo.On("GetDependentJobs", ctx, &deletedJobs[0]).Return([]models.JobSpec{existingJobSpecs[0]}, nil).Once()

			// rest of the jobs with success state
			for i := 1; i < len(deletedJobs); i++ {
				spec := deletedJobs[i]
				jobSpecRepo.On("GetDependentJobs", ctx, &spec).Return([]models.JobSpec{}, nil).Once()
				namespaceJobSpecRepo.On("Delete", ctx, deletedJobs[i].ID).Return(nil)
			}

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, jobSpecRepo, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

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

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				namespaceJobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				namespaceJobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			// one job with failing case
			jobSpecRepo.On("GetDependentJobs", ctx, &deletedJobs[0]).Return([]models.JobSpec{}, nil).Once()
			namespaceJobSpecRepo.On("Delete", ctx, deletedJobs[0].ID).Return(errors.New(errorMsg))

			// rest of the jobs with success state
			for i := 1; i < len(deletedJobs); i++ {
				spec := deletedJobs[i]
				jobSpecRepo.On("GetDependentJobs", ctx, &spec).Return([]models.JobSpec{}, nil).Once()
				namespaceJobSpecRepo.On("Delete", ctx, deletedJobs[i].ID).Return(nil)
			}

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, jobSpecRepo, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				namespaceJobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				namespaceJobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			for _, deletedJob := range deletedJobs {
				spec := deletedJob
				jobSpecRepo.On("GetDependentJobs", ctx, &spec).Return([]models.JobSpec{}, nil).Once()
				namespaceJobSpecRepo.On("Delete", ctx, deletedJob.ID).Return(nil)
			}

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)

			for _, jobSpec := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range modifiedJobs {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(models.DeploymentID{}, errors.New(errorMsg))

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, jobSpecRepo, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
			assert.Equal(t, err.Error(), errorMsg)
		})
		t.Run("should deploy jobs without DependencyMod successfully", func(t *testing.T) {
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

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range modifiedJobs {
				namespaceJobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				namespaceJobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			for _, jobSpec := range deletedJobs {
				spec := jobSpec
				jobSpecRepo.On("GetDependentJobs", ctx, &spec).Return([]models.JobSpec{}, nil).Once()
				namespaceJobSpecRepo.On("Delete", ctx, jobSpec.ID).Return(nil)
			}

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)

			for _, modifiedJob := range modifiedJobs {
				pluginService.On("GenerateDestination", ctx, modifiedJob, namespaceSpec).Return(destination, nil)
			}

			// jobs without dependency mod
			var res models.GenerateDependenciesResponse
			pluginService.On("GenerateDependencies", ctx, modifiedJobs[0], namespaceSpec, false).Return(&res, service.ErrDependencyModNotFound)

			// jobs with dependency mod
			resourceURNs := []string{"source-a"}
			for i := 1; i < len(modifiedJobs); i++ {
				pluginService.On("GenerateDependencies", ctx, modifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, modifiedJobs[i].ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, jobSpecRepo, jobSourceRepo)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
			assert.Nil(t, err)
		})
		t.Run("should deploy successfully", func(t *testing.T) {
			existingJobSpecs := createJobSpecDummy(10)
			for i := range existingJobSpecs {
				existingJobSpecs[i].ID = uuid.New()
			}
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"
			modifiedJobs := requestedJobSpecs[0:1]
			modifiedJobs[0].ID = existingJobSpecs[9].ID
			newRequestedJobs := requestedJobSpecs[1:]
			createAndModifiedJobs := append(newRequestedJobs, modifiedJobs...)
			deletedJobs := existingJobSpecs[:9]

			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
			deployID := models.DeploymentID(uuid.New())

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

			pluginService := new(mock.DependencyResolverPluginService)
			defer pluginService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			jobSourceRepo := new(mock.JobSourceRepository)
			defer jobSourceRepo.AssertExpectations(t)

			depenResolver := new(mock.DependencyResolver)
			defer depenResolver.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			deployManager := new(mock.DeployManager)
			defer deployManager.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepo.On("GetAll", ctx).Return(existingJobSpecs, nil)
			for _, jobSpec := range createAndModifiedJobs {
				namespaceJobSpecRepo.On("Save", ctx, jobSpec, destination.URN()).Return(nil)
				namespaceJobSpecRepo.On("GetByName", ctx, jobSpec.Name).Return(jobSpec, nil)
			}

			for _, jobSpec := range deletedJobs {
				spec := jobSpec
				jobSpecRepo.On("GetDependentJobs", ctx, &spec).Return([]models.JobSpec{}, nil).Once()
				namespaceJobSpecRepo.On("Delete", ctx, jobSpec.ID).Return(nil)
			}

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)

			for _, jobSpec := range createAndModifiedJobs {
				pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
			}

			resourceURNs := []string{"source-a"}
			for i, job := range createAndModifiedJobs {
				pluginService.On("GenerateDependencies", ctx, createAndModifiedJobs[i], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
				jobSourceRepo.On("Save", ctx, projSpec.ID, job.ID, resourceURNs).Return(nil)
			}

			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, namespaceService, nil, deployManager, pluginService, jobSpecRepo, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)
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

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetJobByResourceDestination", ctx, destination).Return(jobSpec1, nil)
			defer jobSpecRepository.AssertExpectations(t)

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
			jobSpecsResult, err := svc.GetByDestination(ctx, projSpec, destination)
			assert.Nil(t, err)
			assert.Equal(t, jobSpec1, jobSpecsResult)
		})
		t.Run("should return error when unable to fetch job spec using destination", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			destination := "resource-urn"

			jobSpecRepository := new(mock.JobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)
			errorMsg := "unable to fetch jobspec"
			jobSpecRepository.On("GetJobByResourceDestination", ctx, destination).Return(models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
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

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projectJobSpecRepo.On("GetAll", ctx).Return(jobSpecs, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			jobSpecRepo.On("GetJobByName", ctx, jobSpec1.GetName()).Return([]models.JobSpec{jobSpec1}, nil)
			jobSpecRepo.On("GetJobByResourceDestination", ctx, destination).Return(jobSpec1, nil)

			svc := job.NewService(nil, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, projectService, nil, nil, jobSpecRepo, nil)
			jobSpecsResult, err := svc.GetByFilter(ctx, models.JobSpecFilter{JobName: jobSpec1.Name})
			assert.Nil(t, err)
			assert.Equal(t, jobSpecs, jobSpecsResult)

			jobSpecsResult1, err := svc.GetByFilter(ctx, models.JobSpecFilter{ResourceDestination: destination})
			assert.Nil(t, err)
			assert.Equal(t, jobSpecs, jobSpecsResult1)

			jobSpecsResult2, err := svc.GetByFilter(ctx, models.JobSpecFilter{ProjectName: jobSpec1.NamespaceSpec.ProjectSpec.Name})
			assert.Nil(t, err)
			assert.Equal(t, jobSpecs, jobSpecsResult2)
		})
		t.Run("should return empty job specs and no error when jobs are not found", func(t *testing.T) {
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

			projectService := new(mock.ProjectService)
			defer projectService.AssertExpectations(t)

			projectJobSpecRepo := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepo.AssertExpectations(t)

			projJobSpecRepoFac := new(mock.ProjectJobSpecRepoFactory)
			defer projJobSpecRepoFac.AssertExpectations(t)

			jobSpecRepo := new(mock.JobSpecRepository)
			defer jobSpecRepo.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)

			jobSpecRepo.On("GetJobByName", ctx, jobSpec1.GetName()).Return([]models.JobSpec{}, nil)
			jobSpecRepo.On("GetJobByResourceDestination", ctx, destination).Return(models.JobSpec{}, store.ErrResourceNotFound)

			svc := job.NewService(nil, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, projectService, nil, nil, jobSpecRepo, nil)
			jobSpecsResult, err := svc.GetByFilter(ctx, models.JobSpecFilter{JobName: jobSpec1.Name})
			assert.NoError(t, err)
			assert.Empty(t, jobSpecsResult)

			jobSpecsResult1, err := svc.GetByFilter(ctx, models.JobSpecFilter{ResourceDestination: destination})
			assert.NoError(t, err)
			assert.Empty(t, jobSpecsResult1)

			jobSpecsResult2, err := svc.GetByFilter(ctx, models.JobSpecFilter{ProjectName: jobSpec1.NamespaceSpec.ProjectSpec.Name})
			assert.NoError(t, err)
			assert.Empty(t, jobSpecsResult2)
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

			svc := job.NewService(nil, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, nil, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, depenResolver, nil, projJobSpecRepoFac, nil, nil, nil, nil, nil, nil, nil)
			jobSpecsResult, err := svc.GetDownstream(ctx, projSpec, destination)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, jobSpecsResult)
		})
	})

	t.Run("Refresh", func(t *testing.T) {
		// TODO: remove it once refresh job changes for refactoring observer is merged
		t.Skip()
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should successfully refresh job specs for a namespace", func(t *testing.T) {
			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)
			namespaceJobSpecRepo.On("GetAll", ctx).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, namespaceNames, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			svc := job.NewService(nil, batchScheduler, nil, dependencyResolver,
				priorityResolver, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, jobNames, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(models.ProjectSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, nil, nil, nil)

			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			projJobSpecRepoFac.On("New", projSpec).Return(projectJobSpecRepo)
			projectJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, nil, nil, nil)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Equal(t, fmt.Sprintf("failed to retrieve jobs: %s", errorMsg), err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to get namespaceSpec when refreshing a namespace", func(t *testing.T) {
			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployManager, nil, nil, nil)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, namespaceNames, nil, logWriter)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to fetch job specs when refreshing a namespace", func(t *testing.T) {
			namespaceJobSpecRepo := new(mock.NamespaceJobSpecRepository)
			defer namespaceJobSpecRepo.AssertExpectations(t)

			namespaceJobSpecRepoFac := new(mock.NamespaceJobSpecRepoFactory)
			defer namespaceJobSpecRepoFac.AssertExpectations(t)

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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceNames := []string{namespaceSpec.Name}

			projectService.On("Get", ctx, projSpec.Name).Return(projSpec, nil)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceJobSpecRepoFac.On("New", namespaceSpec).Return(namespaceJobSpecRepo)
			namespaceJobSpecRepo.On("GetAll", ctx).Return([]models.JobSpec{}, errors.New(errorMsg))

			svc := job.NewService(namespaceJobSpecRepoFac, nil, nil, dependencyResolver,
				nil, nil, nil, namespaceService, projectService, deployManager, nil, nil, nil)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, namespaceNames, nil, logWriter)

			assert.Equal(t, fmt.Sprintf("failed to retrieve jobs: %s", errorMsg), err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projectJobSpecRepoFac, nil, namespaceService, projectService, deployManager, nil, nil, nil)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, jobNames, logWriter)

			assert.Equal(t, fmt.Sprintf("failed to retrieve job: %s", errorMsg), err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
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

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

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

			svc := job.NewService(nil, nil, nil, dependencyResolver,
				nil, projJobSpecRepoFac, nil, namespaceService, projectService, deployManager, pluginService, nil, jobSourceRepo)

			logWriter.On("Write", mock2.Anything, mock2.Anything).Return(nil)
			deployID, err := svc.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
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

			svc := job.NewService(nil, nil, nil, nil,
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

			svc := job.NewService(nil, nil, nil, nil,
				nil, nil, nil, nil, nil, deployManager, nil, nil, nil)
			deployment, err := svc.GetDeployment(ctx, deployID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.JobDeployment{}, deployment)
		})
	})
}
