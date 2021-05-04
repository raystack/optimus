package v1_test

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/odpf/optimus/job"

	"github.com/odpf/optimus/instance"

	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	v1 "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestRuntimeServiceServer(t *testing.T) {
	logger.Init("INFO")
	dumpAssets := func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
		return jobSpec.Assets, nil
	}

	t.Run("Version", func(t *testing.T) {
		t.Run("should save specs and return with data", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				nil, nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			versionRequest := pb.VersionRequest{Client: Version}
			resp, err := runtimeServiceServer.Version(context.TODO(), &versionRequest)
			assert.Nil(t, err)
			assert.Equal(t, Version, resp.Server)
			assert.Equal(t, &pb.VersionResponse{Server: Version}, resp)
		})
	})

	t.Run("RegisterInstance", func(t *testing.T) {
		t.Run("should register a job instance", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"
			jobName := "a-data-job"
			taskName := "a-data-task"

			mockedTimeNow := time.Now()
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			scheduledAtTimestamp, _ := ptypes.TimestampProto(scheduledAt)

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			execUnit1 := new(mock.Transformer)
			execUnit1.On("Name").Return(taskName)
			defer execUnit1.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Unit: execUnit1,
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
					}),
			}

			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", jobName, projectSpec).Return(jobSpec, nil)
			defer jobService.AssertExpectations(t)

			instanceService := new(mock.InstanceService)
			instanceService.On("Register", jobSpec, scheduledAt, models.InstanceTypeTransformation).Return(instanceSpec, nil)
			defer instanceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,

				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				instanceService,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{ProjectName: projectName, JobName: jobName,
				Type: string(models.InstanceTypeTransformation), ScheduledAt: scheduledAtTimestamp}
			resp, err := runtimeServiceServer.RegisterInstance(context.TODO(), &versionRequest)
			assert.Nil(t, err)

			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)
			projectSpecProto := adapter.ToProjectProto(projectSpec)
			jobSpecProto, _ := adapter.ToJobProto(jobSpec)
			instanceSpecProto, _ := adapter.ToInstanceProto(instanceSpec)
			expectedResponse := &pb.RegisterInstanceResponse{Job: jobSpecProto, Instance: instanceSpecProto, Project: projectSpecProto}

			assert.Equal(t, expectedResponse, resp)
		})
	})

	t.Run("RegisterProject", func(t *testing.T) {
		t.Run("should register a project", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", projectSpec).Return(nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobService, nil,
				projectRepoFactory,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.TODO(), &projectRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectResponse{
				Success: true,
				Message: "saved successfully",
			}, resp)
		})
		t.Run("should return error if saving to repository fails", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", projectSpec).Return(errors.New("a random error"))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobService, nil,
				projectRepoFactory,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.TODO(), &projectRequest)
			assert.Equal(t, "rpc error: code = Internal desc = a random error: failed to save project a-data-project", err.Error())
			assert.Nil(t, resp)
		})
	})

	t.Run("RegisterSecret", func(t *testing.T) {
		t.Run("should register a secret successfully", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(nil, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Save", sec).Return(nil)
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobService, nil,
				projectRepoFactory,
				projectSecretRepoFactory,
				adapter,
				nil,
				nil,
				nil,
			)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName: projectSpec.Name,
				SecretName:  "hello",
				Value:       base64.StdEncoding.EncodeToString([]byte("world")),
			}
			resp, err := runtimeServiceServer.RegisterSecret(context.TODO(), &secretRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterSecretResponse{
				Success: true,
			}, resp)
		})
		t.Run("should return error if saving to secret repository fails", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(nil, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Save", sec).Return(errors.New("random error"))
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobService, nil,
				projectRepoFactory,
				projectSecretRepoFactory,
				adapter,
				nil,
				nil,
				nil,
			)

			secretRequest := pb.RegisterSecretRequest{
				ProjectName: projectSpec.Name,
				SecretName:  "hello",
				Value:       base64.StdEncoding.EncodeToString([]byte("world")),
			}
			resp, err := runtimeServiceServer.RegisterSecret(context.TODO(), &secretRequest)
			assert.Nil(t, resp)
			assert.Equal(t, "rpc error: code = Internal desc = random error: failed to save secret hello", err.Error())
		})
	})

	t.Run("DeployJobSpecification", func(t *testing.T) {
		t.Run("should return error if fails to save Job", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"
			jobName1 := "a-data-job"
			taskName := "a-data-task"

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			execUnit1 := new(mock.Transformer)
			execUnit1.On("Name").Return(taskName)

			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: execUnit1,
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
						}),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("Save", mock2.Anything).Return(errors.New("a random error"))
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			jobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer jobSpecRepoFactory.AssertExpectations(t)

			allTasksRepo := new(mock.SupportedTransformationRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				job.NewService(
					jobSpecRepoFactory,
					nil,
					nil,
					nil,
					nil,
					nil,
					nil,
				), nil,
				projectRepoFactory,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			jobSpecsAdapted := []*pb.JobSpecification{}
			for _, jobSpec := range jobSpecs {
				jobSpecAdapted, _ := adapter.ToJobProto(jobSpec)
				jobSpecsAdapted = append(jobSpecsAdapted, jobSpecAdapted)
			}
			deployRequest := pb.DeployJobSpecificationRequest{ProjectName: projectName, Jobs: jobSpecsAdapted}
			err := runtimeServiceServer.DeployJobSpecification(&deployRequest, nil)
			assert.Equal(t, "rpc error: code = Internal desc = failed to save job: a-data-job: a random error: failed to save a-data-job", err.Error())
		})
	})

	t.Run("GetWindow", func(t *testing.T) {
		t.Run("should return the correct window date range", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				nil, nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			scheduledAtTimestamp, _ := ptypes.TimestampProto(scheduledAt)
			req := pb.GetWindowRequest{
				ScheduledAt: scheduledAtTimestamp,
				Size:        "24h",
				Offset:      "24h",
				TruncateTo:  "d",
			}
			resp, err := runtimeServiceServer.GetWindow(context.TODO(), &req)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", ptypes.TimestampString(resp.GetStart()))
			assert.Equal(t, "2020-11-12T00:00:00Z", ptypes.TimestampString(resp.GetEnd()))
		})
		t.Run("should return error if any of the required fields in request is missing", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				nil, nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			scheduledAtTimestamp, _ := ptypes.TimestampProto(scheduledAt)
			req := pb.GetWindowRequest{
				ScheduledAt: scheduledAtTimestamp,
				Size:        "",
				Offset:      "24h",
				TruncateTo:  "d",
			}
			_, err := runtimeServiceServer.GetWindow(context.TODO(), &req)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = window size, offset and truncate_to must be provided", err.Error())
		})
	})

	t.Run("DumpJobSpecification", func(t *testing.T) {
		t.Run("should dump specification of a job", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"
			jobName := "a-data-job"

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			execUnit1 := new(mock.Transformer)
			defer execUnit1.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Unit: execUnit1,
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
					}),
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByName", jobName).Return(jobSpec, nil)
			jobSpecRepository.On("GetAll").Return([]models.JobSpec{jobSpec}, nil)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			jobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer jobSpecRepoFactory.AssertExpectations(t)

			compiler := new(mock.Compiler)
			compiler.On("Compile", jobSpec, projectSpec).Return(models.Job{
				Name:     "name-of-dag",
				Contents: []byte("content-of-dag"),
			}, nil)
			defer compiler.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			dependencyResolver.On("Resolve", projectSpec, jobSpecRepository, jobSpec, nil).Return(jobSpec, nil)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			priorityResolver.On("Resolve", []models.JobSpec{jobSpec}).Return([]models.JobSpec{jobSpec}, nil)
			defer priorityResolver.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				job.NewService(
					jobSpecRepoFactory,
					nil,
					compiler,
					dumpAssets,
					dependencyResolver,
					priorityResolver,
					nil,
				), nil,
				projectRepoFactory,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			req := pb.DumpJobSpecificationRequest{
				ProjectName: projectName,
				JobName:     jobName,
			}
			resp, err := runtimeServiceServer.DumpJobSpecification(context.TODO(), &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
			assert.Equal(t, "content-of-dag", resp.GetContent())
		})
	})

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should create datastore resource successfully", func(t *testing.T) {
			projectName := "a-data-project"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			// prepare mocked datastore
			dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

			dsTypeTableController := new(mock.DatastoreTypeController)
			dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

			dsTypeDatasetController := new(mock.DatastoreTypeController)
			dsTypeDatasetController.On("Adapter").Return(dsTypeTableAdapter)

			dsController := map[models.ResourceType]models.DatastoreTypeController{
				models.ResourceTypeDataset: dsTypeTableController,
			}
			datastorer := new(mock.Datastorer)
			datastorer.On("Types").Return(dsController)
			datastorer.On("Name").Return("bq")

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			dsTypeTableAdapter.On("FromProtobuf", mock2.Anything).Return(resourceSpec, nil)

			req := pb.CreateResourceRequest{
				ProjectName:   projectName,
				DatastoreName: "bq",
				Resource: &pb.ResourceSpecification{
					Version: 1,
					Name:    "proj.datas",
					Type:    models.ResourceTypeDataset.String(),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("CreateResource", context.TODO(), projectSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"Version",
				nil, resourceSvc,
				projectRepoFactory,
				nil,
				v1.NewAdapter(nil, nil, dsRepo),
				nil,
				nil,
				nil,
			)

			resp, err := runtimeServiceServer.CreateResource(context.TODO(), &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})

	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("should update datastore resource successfully", func(t *testing.T) {
			projectName := "a-data-project"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			// prepare mocked datastore
			dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

			dsTypeTableController := new(mock.DatastoreTypeController)
			dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

			dsTypeDatasetController := new(mock.DatastoreTypeController)
			dsTypeDatasetController.On("Adapter").Return(dsTypeTableAdapter)

			dsController := map[models.ResourceType]models.DatastoreTypeController{
				models.ResourceTypeDataset: dsTypeTableController,
			}
			datastorer := new(mock.Datastorer)
			datastorer.On("Types").Return(dsController)
			datastorer.On("Name").Return("bq")

			dsRepo := new(mock.SupportedDatastoreRepo)
			dsRepo.On("GetByName", "bq").Return(datastorer, nil)

			resourceSpec := models.ResourceSpec{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
			}

			dsTypeTableAdapter.On("FromProtobuf", mock2.Anything).Return(resourceSpec, nil)

			req := pb.UpdateResourceRequest{
				ProjectName:   projectName,
				DatastoreName: "bq",
				Resource: &pb.ResourceSpecification{
					Version: 1,
					Name:    "proj.datas",
					Type:    models.ResourceTypeDataset.String(),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("UpdateResource", context.TODO(), projectSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"Version",
				nil, resourceSvc,
				projectRepoFactory,
				nil,
				v1.NewAdapter(nil, nil, dsRepo),
				nil,
				nil,
				nil,
			)

			resp, err := runtimeServiceServer.UpdateResource(context.TODO(), &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})
}
