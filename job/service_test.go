package job_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	tMock "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

func TestService(t *testing.T) {
	ctx := context.Background()

	t.Run("Create", func(t *testing.T) {
		t.Run("should create a new JobSpec and store in repository", func(t *testing.T) {
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}

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
				ResourceDestination: destination.URN(),
				NamespaceSpec:       namespaceSpec,
			}
			enrichedJobSpec := jobSpec
			enrichedJobSpec.ID = uuid.New()

			pluginService := mock.NewPluginService(t)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("Save", ctx, jobSpec).Return(nil)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpec.Name, jobSpec.GetProjectSpec().Name).Return(enrichedJobSpec, nil)

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, pluginService, jobSpecRepository, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.NoError(t, err)
		})

		t.Run("should not fail if dependency module is not found in plugin service", func(t *testing.T) {
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
				NamespaceSpec: namespaceSpec,
			}
			enrichedJobSpec := jobSpec
			enrichedJobSpec.ID = uuid.New()

			pluginService := mock.NewPluginService(t)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(nil, service.ErrDependencyModNotFound)

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("Save", ctx, jobSpec).Return(nil)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpec.Name, jobSpec.GetProjectSpec().Name).Return(enrichedJobSpec, nil)

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, pluginService, jobSpecRepository, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.NoError(t, err)
		})

		t.Run("should fail if saving to repo fails", func(t *testing.T) {
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
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
				ResourceDestination: destination.URN(),
				NamespaceSpec:       namespaceSpec,
			}

			pluginService := mock.NewPluginService(t)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("Save", ctx, jobSpec).Return(errors.New("unknown error"))

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, pluginService, jobSpecRepository, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.Error(t, err)
		})

		t.Run("should fail if getting the updated jobspec failed", func(t *testing.T) {
			destination := &models.GenerateDestinationResponse{
				Destination: "project.dataset.table",
				Type:        models.DestinationTypeBigquery,
			}
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
				ResourceDestination: destination.URN(),
				NamespaceSpec:       namespaceSpec,
			}

			pluginService := mock.NewPluginService(t)
			defer pluginService.AssertExpectations(t)
			pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("Save", ctx, jobSpec).Return(nil)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpec.Name, projSpec.Name).Return(models.JobSpec{}, errors.New("unknown error"))

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, pluginService, jobSpecRepository, nil)
			_, err := svc.Create(ctx, namespaceSpec, jobSpec)
			assert.Error(t, err)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		t.Run("should get jobSpec by the name", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-1",
				ProjectSpec: projSpec,
			}
			jobSpec := models.JobSpec{
				ID:      uuid.Must(uuid.NewUUID()),
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpec.Name, namespaceSpec.ProjectSpec.Name).Return(jobSpec, nil)

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
			_, err := svc.GetByName(ctx, jobSpec.Name, namespaceSpec)

			assert.NoError(t, err)
		})

		t.Run("should fail if repo fail", func(t *testing.T) {
			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "dev-team-1",
				ProjectSpec: projSpec,
			}
			jobSpec := models.JobSpec{
				ID:      uuid.Must(uuid.NewUUID()),
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				NamespaceSpec: namespaceSpec,
			}

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpec.Name, namespaceSpec.ProjectSpec.Name).Return(models.JobSpec{}, errors.New("unknown error"))

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
			_, err := svc.GetByName(ctx, jobSpec.Name, namespaceSpec)

			assert.Error(t, err)
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

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, currentSpec, namespaceSpec, true).
				Return(&models.GenerateDependenciesResponse{}, service.ErrDependencyModNotFound)
			defer pluginService.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("VerifyJob", ctx, namespaceSpec, currentSpec).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			defer logWriter.AssertExpectations(t)

			jobService := job.NewService(batchScheduler, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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

			pluginService := mock.NewPluginService(t)
			pluginService.On("GenerateDependencies", ctx, currentSpec, namespaceSpec, true).
				Return(&models.GenerateDependenciesResponse{}, nil)
			defer pluginService.AssertExpectations(t)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("VerifyJob", ctx, namespaceSpec, currentSpec).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			defer logWriter.AssertExpectations(t)

			jobService := job.NewService(batchScheduler, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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

			pluginService := mock.NewPluginService(t)
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

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, pluginService, nil, nil)
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
					NamespaceSpec:       namespaceSpec,
				},
			}

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetDependentJobs", ctx,
				jobSpecsBase[0].Name, jobSpecsBase[0].GetProjectSpec().Name, jobSpecsBase[0].ResourceDestination,
			).Return([]models.JobSpec{}, nil)
			jobSpecRepository.On("DeleteByID", ctx, jobSpecsBase[0].ID).Return(nil)

			batchScheduler := new(mock.Scheduler)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec.Name, namespaceSpec, []string{jobSpecsBase[0].Name}, nil).Return(nil)
			batchScheduler.On("DeleteJobs", ctx, namespaceSpec.ID.String(), namespaceSpec, []string{jobSpecsBase[0].Name}, nil).Return(nil)
			defer batchScheduler.AssertExpectations(t)

			svc := job.NewService(batchScheduler, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])

			assert.NoError(t, err)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetDependentJobs", ctx,
				jobSpecsBase[0].Name, jobSpecsBase[0].GetProjectSpec().Name, jobSpecsBase[0].ResourceDestination,
			).Return([]models.JobSpec{jobSpecsBase[1]}, nil)

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])

			assert.Error(t, err)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetDependentJobs", ctx,
				jobSpecsBase[0].Name, jobSpecsBase[0].GetProjectSpec().Name, jobSpecsBase[0].ResourceDestination,
			).Return([]models.JobSpec{}, nil)
			jobSpecRepository.On("DeleteByID", ctx, jobSpecsBase[0].ID).Return(errors.New("unknown error"))

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)
			err := svc.Delete(ctx, namespaceSpec, jobSpecsBase[0])

			assert.Error(t, err)
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

		t.Run("should failed when unable to get namespace spec", func(t *testing.T) {
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			defer pluginService.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New("unknown error"))

			svc := job.NewService(nil, nil, nil, nil, nil, namespaceService, nil, nil, pluginService, nil, nil)

			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)

			assert.Error(t, err)
		})

		t.Run("should failed when unable to get all jobs in the namespace when checking diff", func(t *testing.T) {
			requestedJobSpecs := createJobSpecDummy(15)[9:]
			requestedJobSpecs[0].Owner = "optimus-edited"

			namespaceService := new(mock.NamespaceService)
			defer namespaceService.AssertExpectations(t)

			pluginService := mock.NewPluginService(t)
			defer pluginService.AssertExpectations(t)

			logWriter := new(mock.LogWriter)
			defer logWriter.AssertExpectations(t)

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectNameAndNamespaceName", ctx, namespaceSpec.ProjectSpec.Name, namespaceSpec.Name).Return(nil, errors.New("unknown error"))

			namespaceService.On("Get", ctx, projSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)

			svc := job.NewService(nil, nil, nil, nil, nil, namespaceService, nil, nil, pluginService, jobSpecRepository, nil)

			_, err := svc.Deploy(ctx, projSpec.Name, namespaceSpec.Name, requestedJobSpecs, logWriter)

			assert.Error(t, err)
		})

		t.Run("should delete existing jobs if they are missing from the incoming ones and return deployment id and nil", func(t *testing.T) {
			namespaceService := new(mock.NamespaceService)
			deployManager := new(mock.DeployManager)
			pluginService := mock.NewPluginService(t)
			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobService := job.NewService(nil, nil, nil, nil, nil, namespaceService, nil, deployManager, pluginService, jobSpecRepository, nil)

			projetSpec := models.ProjectSpec{
				Name: "project",
			}
			namespaceSpec := models.NamespaceSpec{
				Name:        "namespace",
				ProjectSpec: projetSpec,
			}

			ctx := context.Background()
			inputProjectName := "project"
			inputNamespaceName := "namespace"
			inputJobSpecs := []models.JobSpec{
				{
					Name:          "job1",
					NamespaceSpec: namespaceSpec,
				},
			}

			namespaceService.On("Get", ctx, inputProjectName, inputNamespaceName).Return(namespaceSpec, nil)

			existingJobSpecs := []models.JobSpec{
				{
					Name:          "job1",
					NamespaceSpec: namespaceSpec,
				},
				{
					Name:          "job3",
					NamespaceSpec: namespaceSpec,
				},
			}
			jobSpecRepository.On("GetAllByProjectNameAndNamespaceName", ctx, inputProjectName, inputNamespaceName).Return(existingJobSpecs, nil)

			logWriter := new(mock.LogWriter)
			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)

			jobSpecRepository.On("GetDependentJobs", ctx, tMock.Anything, inputProjectName, tMock.Anything).Return([]models.JobSpec{}, nil)
			jobSpecRepository.On("DeleteByID", ctx, tMock.Anything).Return(nil)

			expectedDeploymentID := models.DeploymentID(uuid.New())
			deployManager.On("Deploy", ctx, tMock.Anything).Return(expectedDeploymentID, nil)

			actualDeploymentID, actualError := jobService.Deploy(ctx, inputProjectName, inputNamespaceName, inputJobSpecs, logWriter)

			assert.EqualValues(t, expectedDeploymentID, actualDeploymentID)
			assert.NoError(t, actualError)
		})
	})

	t.Run("GetByDestination", func(t *testing.T) {
		t.Run("should return empty and error if error getting job by destination", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			destination := "destination_test"

			jobSpecRepository.On("GetByResourceDestinationURN", ctx, destination).Return(models.JobSpec{}, errors.New("unknown error"))

			actualJobSpec, actualError := svc.GetByDestination(ctx, projSpec, destination)

			assert.Error(t, actualError)
			assert.Empty(t, actualJobSpec)
		})

		t.Run("should return job spec and nil if found within the same project", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			projSpec := models.ProjectSpec{
				Name: "proj",
			}
			namespaceSpec := models.NamespaceSpec{
				ProjectSpec: projSpec,
			}
			jobSpec := models.JobSpec{
				NamespaceSpec: namespaceSpec,
			}
			destination := "destination_test"

			jobSpecRepository.On("GetByResourceDestinationURN", ctx, destination).Return(jobSpec, nil)

			actualJobSpec, actualError := svc.GetByDestination(ctx, projSpec, destination)

			assert.NoError(t, actualError)
			assert.NotEmpty(t, actualJobSpec)
		})

		t.Run("should return empty and error if found but in different project", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			projSpec1 := models.ProjectSpec{
				Name: "proj1",
			}
			projSpec2 := models.ProjectSpec{
				Name: "proj2",
			}
			namespaceSpec := models.NamespaceSpec{
				ProjectSpec: projSpec1,
			}
			jobSpec := models.JobSpec{
				NamespaceSpec: namespaceSpec,
			}
			destination := "destination_test"

			jobSpecRepository.On("GetByResourceDestinationURN", ctx, destination).Return(jobSpec, nil)

			actualJobSpec, actualError := svc.GetByDestination(ctx, projSpec2, destination)

			assert.Error(t, actualError)
			assert.Empty(t, actualJobSpec)
		})
	})

	t.Run("GetByFilter", func(t *testing.T) {
		t.Run("should return nil and error if resource destination is set but error is encountered", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ResourceDestination: "destination_test",
			}

			jobSpecRepository.On("GetByResourceDestinationURN", ctx, filter.ResourceDestination).Return(models.JobSpec{}, errors.New("unknown error"))

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("should return empty and nil if resource destination is set but no records are found", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ResourceDestination: "destination_test",
			}

			jobSpecRepository.On("GetByResourceDestinationURN", ctx, filter.ResourceDestination).Return(models.JobSpec{}, store.ErrResourceNotFound)

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.Empty(t, actualJobSpecs)
			assert.NoError(t, actualError)
		})

		t.Run("should return spec and nil if resource destination is set and found records", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ResourceDestination: "destination_test",
			}

			jobSpecRepository.On("GetByResourceDestinationURN", ctx, filter.ResourceDestination).Return(models.JobSpec{}, nil)

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.NotEmpty(t, actualJobSpecs)
			assert.NoError(t, actualError)
		})

		t.Run("should return the result from repository if project name is set but resource name and job name are not", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ProjectName: "project_test",
			}

			jobSpecRepository.On("GetAllByProjectName", ctx, filter.ProjectName).Return([]models.JobSpec{}, nil)

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.Empty(t, actualJobSpecs)
			assert.NoError(t, actualError)
		})

		t.Run("should return nil and error if both project name and job name are set and resource name is not, and error when getting from repository", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ProjectName: "project_test",
				JobName:     "job_test",
			}

			jobSpecRepository.On("GetByNameAndProjectName", ctx, filter.JobName, filter.ProjectName).Return(models.JobSpec{}, errors.New("unknown error"))

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("should return empty and nil if both project name and job name are set and resource name is not, and job is not found", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ProjectName: "project_test",
				JobName:     "job_test",
			}

			jobSpecRepository.On("GetByNameAndProjectName", ctx, filter.JobName, filter.ProjectName).Return(models.JobSpec{}, store.ErrResourceNotFound)

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.Empty(t, actualJobSpecs)
			assert.NoError(t, actualError)
		})

		t.Run("should return spec and nil if both project name and job name are set and resource name is not, and job is found", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{
				ProjectName: "project_test",
				JobName:     "job_test",
			}

			jobSpecRepository.On("GetByNameAndProjectName", ctx, filter.JobName, filter.ProjectName).Return(models.JobSpec{}, nil)

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.NotEmpty(t, actualJobSpecs)
			assert.NoError(t, actualError)
		})

		t.Run("should return nil and error if the filters are not set", func(t *testing.T) {
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)

			ctx := context.Background()
			filter := models.JobSpecFilter{}

			actualJobSpecs, actualError := service.GetByFilter(ctx, filter)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})
	})

	t.Run("GetDownstream", func(t *testing.T) {
		t.Run("should return nil and error if error is encountered when getting all jobs within a project", func(t *testing.T) {
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			projectSpec := models.ProjectSpec{
				Name: "project_tes",
			}
			jobName := "job_test"

			jobSpecRepository.On("GetAllByProjectName", ctx, projectSpec.Name).Return(nil, errors.New("unknown error"))

			actualJobSpecs, actualError := service.GetDownstream(ctx, projectSpec, jobName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("should return nil and error if error is encountered when resolving dependency for one or more jobs", func(t *testing.T) {
			dependencyResolver := mock.NewDependencyResolver(t)
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, dependencyResolver, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			projectSpec := models.ProjectSpec{
				Name: "project_tes",
			}
			jobName := "job_test"

			jobsWithinProject := []models.JobSpec{
				{Name: "job_1", NamespaceSpec: models.NamespaceSpec{Name: "namespace_1"}},
				{Name: "job_2", NamespaceSpec: models.NamespaceSpec{Name: "namespace_1"}},
				{Name: "job_3", NamespaceSpec: models.NamespaceSpec{Name: "namespace_2"}},
			}

			jobSpecRepository.On("GetAllByProjectName", ctx, projectSpec.Name).Return(jobsWithinProject, nil)
			for i := range jobsWithinProject {
				dependencyResolver.On("Resolve", ctx, projectSpec, jobsWithinProject[i], nil).Return(models.JobSpec{}, errors.New("unknown error"))
			}

			actualJobSpecs, actualError := service.GetDownstream(ctx, projectSpec, jobName)

			assert.Nil(t, actualJobSpecs)
			assert.Error(t, actualError)
		})

		t.Run("should return job specs and nil if no error is found", func(t *testing.T) {
			dependencyResolver := mock.NewDependencyResolver(t)
			jobSpecRepository := mock.NewJobSpecRepository(t)
			service := job.NewService(nil, nil, dependencyResolver, nil, nil, nil, nil, nil, nil, jobSpecRepository, nil)

			ctx := context.Background()
			projectSpec := models.ProjectSpec{
				Name: "project_tes",
			}
			jobName := "job_1"

			namespaceSpec := models.NamespaceSpec{
				Name:        "namespace_1",
				ProjectSpec: projectSpec,
			}
			jobSpecWithoutDependency := models.JobSpec{
				Name:          "job_1",
				NamespaceSpec: namespaceSpec,
			}
			jobSpecWithDependency := models.JobSpec{
				Name:          "job_2",
				NamespaceSpec: namespaceSpec,
				Dependencies: map[string]models.JobSpecDependency{
					"job_1": {Project: &projectSpec, Job: &jobSpecWithoutDependency, Type: models.JobSpecDependencyTypeIntra},
				},
			}
			jobsWithinProject := []models.JobSpec{jobSpecWithoutDependency, jobSpecWithDependency}

			jobSpecRepository.On("GetAllByProjectName", ctx, projectSpec.Name).Return(jobsWithinProject, nil)
			for i := range jobsWithinProject {
				dependencyResolver.On("Resolve", ctx, projectSpec, jobsWithinProject[i], nil).Return(jobsWithinProject[i], nil)
			}

			actualJobSpecs, actualError := service.GetDownstream(ctx, projectSpec, jobName)

			assert.NotEmpty(t, actualJobSpecs)
			assert.NoError(t, actualError)
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

			pluginService := mock.NewPluginService(t)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectName", ctx, projSpec.Name).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, pluginService, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should successfully refresh job specs for a namespace", func(t *testing.T) {
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

			pluginService := mock.NewPluginService(t)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectNameAndNamespaceName", ctx, namespaceSpec.ProjectSpec.Name, namespaceSpec.Name).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, pluginService, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, namespaceNames, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should successfully refresh job specs for the selected jobs", func(t *testing.T) {
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

			pluginService := mock.NewPluginService(t)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobNames[0], projSpec.Name).Return(jobSpecsBase[0], nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			service := job.NewService(nil, batchScheduler, dependencyResolver, priorityResolver, nil, namespaceService, projectService, deployManager, pluginService, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, jobNames, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to get project spec", func(t *testing.T) {
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

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, nil, nil, jobSourceRepo)

			deployID, err := service.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to fetch job specs when refreshing whole project", func(t *testing.T) {
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectName", ctx, projSpec.Name).Return([]models.JobSpec{}, errors.New(errorMsg))

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, nil, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Error(t, err)
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to get namespaceSpec when refreshing a namespace", func(t *testing.T) {
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

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, nil, nil, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, namespaceNames, nil, logWriter)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to fetch job specs when refreshing a namespace", func(t *testing.T) {
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectNameAndNamespaceName", ctx, namespaceSpec.ProjectSpec.Name, namespaceSpec.Name).Return([]models.JobSpec{}, errors.New(errorMsg))

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, nil, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, namespaceNames, nil, logWriter)

			assert.Equal(t, fmt.Sprintf("failed to retrieve jobs: %s", errorMsg), err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to fetch job specs when refreshing selected jobs", func(t *testing.T) {
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpecsBase[0].Name, projSpec.Name).Return(models.JobSpec{}, errors.New(errorMsg))

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, nil, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, jobNames, logWriter)

			assert.Equal(t, fmt.Sprintf("failed to retrieve job: %s", errorMsg), err.Error())
			assert.Equal(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should not failed refresh when one of the generating dependency call failed", func(t *testing.T) {
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

			pluginService := mock.NewPluginService(t)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectName", ctx, projSpec.Name).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil).Once()
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil).Once()

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[1], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, errors.New(errorMsg))

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, pluginService, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should not failed refresh when one of persisting dependency process failed", func(t *testing.T) {
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

			pluginService := mock.NewPluginService(t)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectName", ctx, projSpec.Name).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[1], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs2}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobSpecsBase[1].ID, resourceURNs2).Return(errors.New(errorMsg))

			deployManager.On("Deploy", ctx, projSpec).Return(deployID, nil)

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, pluginService, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

			assert.Nil(t, err)
			assert.NotEqual(t, models.DeploymentID(uuid.Nil), deployID)
		})
		t.Run("should failed when unable to create deployment request to deploy manager", func(t *testing.T) {
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

			pluginService := mock.NewPluginService(t)
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

			jobSpecRepository := mock.NewJobSpecRepository(t)
			jobSpecRepository.On("GetAllByProjectName", ctx, projSpec.Name).Return(jobSpecsBase, nil)

			pluginService.On("GenerateDependencies", ctx, jobSpecsBase[0], namespaceSpec, false).Return(&models.GenerateDependenciesResponse{Dependencies: resourceURNs}, nil)
			jobSourceRepo.On("Save", ctx, projSpec.ID, jobID, resourceURNs).Return(nil)

			deployManager.On("Deploy", ctx, projSpec).Return(models.DeploymentID{}, errors.New(errorMsg))

			service := job.NewService(nil, nil, dependencyResolver, nil, nil, namespaceService, projectService, deployManager, pluginService, jobSpecRepository, jobSourceRepo)

			logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
			deployID, err := service.Refresh(ctx, projSpec.Name, nil, nil, logWriter)

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

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, deployManager, nil, nil, nil)
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

			svc := job.NewService(nil, nil, nil, nil, nil, nil, nil, deployManager, nil, nil, nil)
			deployment, err := svc.GetDeployment(ctx, deployID)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.JobDeployment{}, deployment)
		})
	})

	t.Run("CreateAndDeploy", func(t *testing.T) {
		destination := &models.GenerateDestinationResponse{
			Destination: "project.dataset.table",
			Type:        models.DestinationTypeBigquery,
		}
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
			NamespaceSpec:       namespaceSpec,
			Dependencies:        map[string]models.JobSpecDependency{},
			ResourceDestination: destination.URN(),
		}
		jobSpecs := []models.JobSpec{jobSpec}

		enrichedJobSpec := jobSpec
		enrichedJobSpec.ID = uuid.New()

		batchScheduler := new(mock.Scheduler)
		batchScheduler.On("VerifyJob", ctx, namespaceSpec, jobSpec).Return(nil)
		defer batchScheduler.AssertExpectations(t)

		pluginService := mock.NewPluginService(t)
		defer pluginService.AssertExpectations(t)

		pluginService.On("GenerateDestination", ctx, jobSpec, namespaceSpec).Return(destination, nil)
		pluginService.On("GenerateDependencies", ctx, jobSpec, namespaceSpec, true).Return(&models.GenerateDependenciesResponse{}, nil)
		pluginService.On("GenerateDependencies", ctx, enrichedJobSpec, namespaceSpec, false).Return(&models.GenerateDependenciesResponse{}, nil)

		logWriter := new(mock.LogWriter)
		logWriter.On("Write", tMock.Anything, tMock.Anything).Return(nil)
		defer logWriter.AssertExpectations(t)

		jobSpecRepository := mock.NewJobSpecRepository(t)
		jobSpecRepository.On("Save", ctx, jobSpec).Return(nil)
		jobSpecRepository.On("GetByNameAndProjectName", ctx, jobSpec.Name, jobSpec.GetProjectSpec().Name).Return(enrichedJobSpec, nil)

		deployManager := new(mock.DeployManager)
		defer deployManager.AssertExpectations(t)

		t.Run("should able to create , persist job sources and schedule deployment", func(t *testing.T) {
			deployID := models.DeploymentID(uuid.New())
			deployManager.On("Deploy", ctx, namespaceSpec.ProjectSpec).Return(deployID, nil)

			svc := job.NewService(batchScheduler, nil, nil, nil, nil, nil, nil, deployManager, pluginService, jobSpecRepository, nil)

			deploymentID, err := svc.CreateAndDeploy(ctx, namespaceSpec, jobSpecs, logWriter)

			assert.Nil(t, err)
			assert.Equal(t, deployID, deploymentID)
		})
	})
}
