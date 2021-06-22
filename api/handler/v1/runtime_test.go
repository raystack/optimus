package v1_test

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/odpf/optimus/core/tree"

	"github.com/odpf/optimus/instance"

	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/core/logger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestRuntimeServiceServer(t *testing.T) {
	logger.Init("INFO")

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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "namespace-124",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			execUnit1 := new(mock.TaskPlugin)
			execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
				Name: taskName,
			}, nil)
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
			//jobService.On("GetByName", jobName, projectSpec).Return(jobSpec, nil)
			jobService.On("GetByNameForProject", jobName, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			instanceService := new(mock.InstanceService)
			instanceService.On("Register", jobSpec, scheduledAt, models.InstanceTypeTask).Return(instanceSpec, nil)
			instanceService.On("Compile", namespaceSpec, jobSpec, instanceSpec, models.InstanceTypeTask, "test").Return(
				map[string]string{
					instance.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					instance.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					instance.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				},
				map[string]string{
					"query.sql": "select * from 1",
				}, nil)
			defer instanceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,
				nil,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				instanceService,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{ProjectName: projectName, JobName: jobName,
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[strings.ToUpper(string(models.InstanceTypeTask))]),
				ScheduledAt:  scheduledAtTimestamp,
				InstanceName: "test",
			}
			resp, err := runtimeServiceServer.RegisterInstance(context.TODO(), &versionRequest)
			assert.Nil(t, err)

			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)
			projectSpecProto := adapter.ToProjectProto(projectSpec)
			jobSpecProto, _ := adapter.ToJobProto(jobSpec)
			instanceSpecProto, _ := adapter.ToInstanceProto(instanceSpec)
			expectedResponse := &pb.RegisterInstanceResponse{
				Job: jobSpecProto, Instance: instanceSpecProto,
				Project: projectSpecProto,
				Context: &pb.InstanceContext{
					Envs: map[string]string{
						instance.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						instance.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						instance.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					Files: map[string]string{
						"query.sql": "select * from 1",
					},
				},
				Namespace: adapter.ToNamespaceProto(namespaceSpec),
			}

			assert.Equal(t, expectedResponse, resp)
		})
	})

	t.Run("RegisterProject", func(t *testing.T) {
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
		t.Run("should register a project without a namespace", func(t *testing.T) {
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
		t.Run("should register a project with a namespace", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				Name:   "dev-test-namespace-1",
				Config: map[string]string{},
			}

			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", projectSpec).Return(nil)
			projectRepository.On("GetByName", projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSvc := new(mock.JobService)
			defer jobSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", namespaceSpec).Return(nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobSvc,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{
				Project:   adapter.ToProjectProto(projectSpec),
				Namespace: adapter.ToNamespaceProto(namespaceSpec),
			}
			resp, err := runtimeServiceServer.RegisterProject(context.TODO(), &projectRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectResponse{
				Success: true,
				Message: "saved successfully",
			}, resp)
		})
	})

	t.Run("RegisterProjectNamespace", func(t *testing.T) {
		t.Run("should save a new namespace", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				Name:   "dev-test-namespace-1",
				Config: map[string]string{},
			}

			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSvc := new(mock.JobService)
			defer jobSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", namespaceSpec).Return(nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobSvc,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			resp, err := runtimeServiceServer.RegisterProjectNamespace(context.TODO(), &namespaceRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectNamespaceResponse{
				Success: true,
				Message: "saved successfully",
			}, resp)
		})
		t.Run("should throw error if project does not exist while saving a namespace", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				Name: "dev-test-namespace-1",
			}

			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectSpec.Name).Return(projectSpec, errors.New("project does not exist"))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSvc := new(mock.JobService)
			defer jobSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobSvc,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			_, err := runtimeServiceServer.RegisterProjectNamespace(context.TODO(), &namespaceRequest)
			assert.NotNil(t, err)
			assert.Equal(t, "rpc error: code = NotFound desc = project does not exist: project a-data-project not found", err.Error())
		})
	})

	t.Run("RegisterJobSpecification", func(t *testing.T) {
		t.Run("should save a job specification", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				Name:        "dev-test-namespace-1",
				ProjectSpec: projectSpec,
			}

			jobName := "my-job"
			taskName := "bq2bq"
			execUnit1 := new(mock.TaskPlugin)
			execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
				Name:  taskName,
				Image: "random-image",
			}, nil)
			execUnit1.On("DefaultTaskAssets", context.Background(), mock2.Anything).Return(models.DefaultTaskAssetsResponse{}, nil)
			defer execUnit1.AssertExpectations(t)
			_ = models.TaskRegistry.Add(execUnit1)

			jobSpec := models.JobSpec{
				Name: jobName,
				Task: models.JobSpecTask{
					Unit: execUnit1,
					Config: models.JobSpecConfigs{
						{
							Name:  "DO",
							Value: "THIS",
						},
					},
					Window: models.JobSpecTaskWindow{
						Size:       time.Hour,
						Offset:     0,
						TruncateTo: "d",
					},
				},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from 1",
						},
					}),
				Dependencies: map[string]models.JobSpecDependency{},
			}

			adapter := v1.NewAdapter(models.TaskRegistry, nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSvc := new(mock.JobService)
			jobSvc.On("Create", jobSpec, namespaceSpec).Return(nil)
			jobSvc.On("Check", namespaceSpec, []models.JobSpec{jobSpec}, mock2.Anything).Return(nil)
			jobSvc.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				jobSvc,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			jobProto, _ := adapter.ToJobProto(jobSpec)
			request := pb.CreateJobSpecificationRequest{
				ProjectName: projectName,
				Namespace:   namespaceSpec.Name,
				Spec:        jobProto,
			}
			resp, err := runtimeServiceServer.CreateJobSpecification(context.TODO(), &request)
			assert.Nil(t, err)
			assert.Equal(t, &pb.CreateJobSpecificationResponse{
				Success: true,
				Message: "job my-job is created and deployed successfully on project a-data-project",
			}, resp)
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
				nil,
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
				nil,
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
		t.Run("should deploy the job", func(t *testing.T) {
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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}

			execUnit1 := new(mock.TaskPlugin)
			execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
				Name: taskName,
			}, nil)
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
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFactory.AssertExpectations(t)

			allTasksRepo := new(mock.SupportedTaskRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("Create", mock2.Anything, namespaceSpec).Return(nil)
			jobService.On("KeepOnly", namespaceSpec, mock2.Anything, mock2.Anything).Return(nil)
			jobService.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobService.AssertExpectations(t)

			grpcRespStream := new(mock.RuntimeService_DeployJobSpecificationServer)
			grpcRespStream.On("Context").Return(context.TODO())
			defer grpcRespStream.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
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
			deployRequest := pb.DeployJobSpecificationRequest{ProjectName: projectName, Jobs: jobSpecsAdapted, Namespace: namespaceSpec.Name}
			err := runtimeServiceServer.DeployJobSpecification(&deployRequest, grpcRespStream)
			assert.Nil(t, err)
		})
	})

	t.Run("ReadJobSpecification", func(t *testing.T) {
		t.Run("should read a job spec", func(t *testing.T) {
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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}

			execUnit1 := new(mock.TaskPlugin)
			execUnit1.On("GetTaskSchema", context.Background(), models.GetTaskSchemaRequest{}).Return(models.GetTaskSchemaResponse{
				Name: taskName,
			}, nil)
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

			allTasksRepo := new(mock.SupportedTaskRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			jobSpecAdapted, _ := adapter.ToJobProto(jobSpecs[0])
			deployRequest := pb.ReadJobSpecificationRequest{ProjectName: projectName, JobName: jobSpecs[0].Name, Namespace: namespaceSpec.Name}
			jobSpecResp, err := runtimeServiceServer.ReadJobSpecification(context.TODO(), &deployRequest)
			assert.Nil(t, err)
			assert.Equal(t, jobSpecAdapted, jobSpecResp.Spec)
		})
	})

	t.Run("ListProjectNamespaces", func(t *testing.T) {
		t.Run("should read namespaces of a project", func(t *testing.T) {
			Version := "1.0.1"

			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			allTasksRepo := new(mock.SupportedTaskRepo)
			adapter := v1.NewAdapter(allTasksRepo, nil, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetAll").Return([]models.NamespaceSpec{namespaceSpec}, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			namespaceAdapted := adapter.ToNamespaceProto(namespaceSpec)
			request := pb.ListProjectNamespacesRequest{ProjectName: projectName}
			resp, err := runtimeServiceServer.ListProjectNamespaces(context.TODO(), &request)
			assert.Nil(t, err)
			assert.Equal(t, []*pb.NamespaceSpecification{namespaceAdapted}, resp.GetNamespaces())
		})
	})

	t.Run("DeleteJobSpecification", func(t *testing.T) {
		t.Run("should delete the job", func(t *testing.T) {
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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}

			execUnit1 := new(mock.TaskPlugin)
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

			allTasksRepo := new(mock.SupportedTaskRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobSpec := jobSpecs[0]

			jobService := new(mock.JobService)
			jobService.On("GetByName", jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			jobService.On("Delete", mock2.Anything, namespaceSpec, jobSpec).Return(nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			deployRequest := pb.DeleteJobSpecificationRequest{ProjectName: projectName, JobName: jobSpec.Name, Namespace: namespaceSpec.Name}
			resp, err := runtimeServiceServer.DeleteJobSpecification(context.TODO(), &deployRequest)
			assert.Nil(t, err)
			assert.Equal(t, "job a-data-job has been deleted", resp.GetMessage())
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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}

			compiledJob := models.Job{
				Name:        jobName,
				NamespaceID: namespaceSpec.ID.String(),
				Contents:    []byte("content-of-dag"),
			}

			execUnit1 := new(mock.TaskPlugin)
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

			jobService := new(mock.JobService)
			jobService.On("GetByName", jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Dump", namespaceSpec, jobSpec).Return(compiledJob, nil)
			defer jobService.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFactory.AssertExpectations(t)

			compiler := new(mock.Compiler)
			defer compiler.AssertExpectations(t)

			dependencyResolver := new(mock.DependencyResolver)
			defer dependencyResolver.AssertExpectations(t)

			priorityResolver := new(mock.PriorityResolver)
			defer priorityResolver.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				jobService,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(models.TaskRegistry, nil, nil),
				nil,
				nil,
				nil,
			)

			req := pb.DumpJobSpecificationRequest{
				ProjectName: projectName,
				JobName:     jobName,
				Namespace:   namespaceSpec.Name,
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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
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
				Namespace: namespaceSpec.Name,
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("CreateResource", context.TODO(), namespaceSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"Version",
				nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
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

			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
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
				Namespace: namespaceSpec.Name,
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("UpdateResource", context.TODO(), namespaceSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"Version",
				nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
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

	t.Run("ReplayDryRun", func(t *testing.T) {
		t.Run("should update datastore resource successfully", func(t *testing.T) {
			projectName := "a-data-project"
			jobName := "a-data-job"
			timeLayout := "2006-01-02"
			startDate, _ := time.Parse(timeLayout, "2020-11-25")
			endDate, _ := time.Parse(timeLayout, "2020-11-28")
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "dev-test-namespace-1",
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
				ProjectSpec: projectSpec,
			}
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
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
			replayRequestInput := &models.ReplayRequestInput{
				Job:     jobSpec,
				Start:   startDate,
				End:     endDate,
				Project: projectSpec,
			}
			dagNode := tree.NewTreeNode(jobSpec)

			jobService := new(mock.JobService)
			jobService.On("GetByName", jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", replayRequestInput).Return(dagNode, nil)
			defer jobService.AssertExpectations(t)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"Version",
				jobService,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName: projectName,
				Namespace:   namespaceSpec.Name,
				JobName:     jobName,
				StartDate:   startDate.Format(timeLayout),
				EndDate:     endDate.Format(timeLayout),
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.TODO(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, true, replayResponse.Success)
			expectedReplayResponse, err := adapter.ToReplayExecutionTreeNode(dagNode)
			assert.Nil(t, err)
			assert.Equal(t, expectedReplayResponse.JobName, replayResponse.Response.JobName)
			assert.Equal(t, expectedReplayResponse.Dependents, replayResponse.Response.Dependents)
			assert.Equal(t, expectedReplayResponse.Runs, replayResponse.Response.Runs)
		})
	})
}
