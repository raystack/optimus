package v1beta1_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/core/set"
	"github.com/odpf/optimus/run"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/utils"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/odpf/optimus/core/tree"

	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
)

func TestRuntimeServiceServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("Version", func(t *testing.T) {
		t.Run("should save specs and return with data", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				nil, nil, nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			versionRequest := pb.VersionRequest{Client: Version}
			resp, err := runtimeServiceServer.Version(context.Background(), &versionRequest)
			assert.Nil(t, err)
			assert.Equal(t, Version, resp.Server)
			assert.Equal(t, &pb.VersionResponse{Server: Version}, resp)
		})
	})

	t.Run("RegisterInstance", func(t *testing.T) {
		Version := "1.0.1"

		projectName := "a-data-project"
		jobName := "a-data-job"
		taskName := "a-data-task"

		mockedTimeNow := time.Now()
		scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
		scheduledAtTimestamp := timestamppb.New(scheduledAt)

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

		basePlugin1 := new(mock.BasePlugin)
		basePlugin1.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name: taskName,
		}, nil)
		defer basePlugin1.AssertExpectations(t)

		jobSpec := models.JobSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: jobName,
			Task: models.JobSpecTask{
				Unit: &models.Plugin{
					Base: basePlugin1,
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
				}),
		}

		instanceSpec := models.InstanceSpec{
			Name:   "do-this",
			Type:   models.InstanceTypeTask,
			Status: models.RunStateRunning,
			Data: []models.InstanceSpecData{
				{
					Name:  run.ConfigKeyExecutionTime,
					Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  run.ConfigKeyDstart,
					Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  run.ConfigKeyDend,
					Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
			},
			ExecutedAt: scheduledAt,
		}

		jobRun := models.JobRun{
			ID:          uuid.Must(uuid.NewRandom()),
			Spec:        jobSpec,
			Trigger:     models.TriggerManual,
			Status:      models.RunStateAccepted,
			ScheduledAt: scheduledAt,
		}
		t.Run("should register a new job instance with run for scheduled triggers", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobName, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			instanceService := new(mock.RunService)
			instanceService.On("GetScheduledRun", ctx, namespaceSpec, jobSpec, scheduledAt).Return(jobRun, nil)
			instanceService.On("Register", ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name).Return(instanceSpec, nil)
			instanceService.On("Compile", ctx, namespaceSpec, jobRun, instanceSpec).Return(
				map[string]string{
					run.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					run.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					run.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				},
				map[string]string{
					"query.sql": "select * from 1",
				}, nil)
			defer instanceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				projectRepoFactory,
				nil,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				instanceService,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{ProjectName: projectName, JobName: jobName,
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
				ScheduledAt:  scheduledAtTimestamp,
				InstanceName: instanceSpec.Name,
			}
			resp, err := runtimeServiceServer.RegisterInstance(context.Background(), &versionRequest)
			assert.Nil(t, err)

			adapter := v1.NewAdapter(nil, nil)
			projectSpecProto := adapter.ToProjectProto(projectSpec)
			jobSpecProto, _ := adapter.ToJobProto(jobSpec)
			instanceSpecProto, _ := adapter.ToInstanceProto(instanceSpec)
			expectedResponse := &pb.RegisterInstanceResponse{
				Job: jobSpecProto, Instance: instanceSpecProto,
				Project: projectSpecProto,
				Context: &pb.InstanceContext{
					Envs: map[string]string{
						run.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					Files: map[string]string{
						"query.sql": "select * from 1",
					},
				},
				Namespace: adapter.ToNamespaceProto(namespaceSpec),
			}

			assert.Equal(t, expectedResponse, resp)
		})
		t.Run("should find the existing job run if manually triggered", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			instanceService := new(mock.RunService)
			instanceService.On("GetByID", ctx, jobRun.ID).Return(jobRun, namespaceSpec, nil)
			instanceService.On("Register", ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name).Return(instanceSpec, nil)
			instanceService.On("Compile", ctx, namespaceSpec, jobRun, instanceSpec).Return(
				map[string]string{
					run.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					run.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					run.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
				},
				map[string]string{
					"query.sql": "select * from 1",
				}, nil)
			defer instanceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"",
				nil,
				nil,
				nil,
				projectRepoFactory,
				nil,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				instanceService,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{
				ProjectName:  projectName,
				JobrunId:     jobRun.ID.String(),
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
				InstanceName: instanceSpec.Name,
			}
			resp, err := runtimeServiceServer.RegisterInstance(context.Background(), &versionRequest)
			assert.Nil(t, err)

			adapter := v1.NewAdapter(nil, nil)
			projectSpecProto := adapter.ToProjectProto(projectSpec)
			jobSpecProto, _ := adapter.ToJobProto(jobSpec)
			instanceSpecProto, _ := adapter.ToInstanceProto(instanceSpec)
			expectedResponse := &pb.RegisterInstanceResponse{
				Job: jobSpecProto, Instance: instanceSpecProto,
				Project: projectSpecProto,
				Context: &pb.InstanceContext{
					Envs: map[string]string{
						run.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
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
			adapter := v1.NewAdapter(nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", ctx, projectSpec).Return(errors.New("a random error"))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobService, nil, nil,
				projectRepoFactory,

				nil,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.Background(), &projectRequest)
			assert.Equal(t, "rpc error: code = Internal desc = a random error: failed to save project a-data-project", err.Error())
			assert.Nil(t, resp)
		})
		t.Run("should register a project", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", ctx, projectSpec).Return(nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobService, nil, nil,
				projectRepoFactory,

				nil,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.Background(), &projectRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectResponse{
				Success: true,
				Message: "project saved successfully.",
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

			adapter := v1.NewAdapter(nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSvc := new(mock.JobService)
			defer jobSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("Save", ctx, namespaceSpec).Return(nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobSvc,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			resp, err := runtimeServiceServer.RegisterProjectNamespace(context.Background(), &namespaceRequest)
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

			adapter := v1.NewAdapter(nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, errors.New("project does not exist"))
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
				log,
				"someVersion1.0",
				jobSvc,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
			)

			namespaceRequest := pb.RegisterProjectNamespaceRequest{
				ProjectName: projectName,
				Namespace:   adapter.ToNamespaceProto(namespaceSpec),
			}
			_, err := runtimeServiceServer.RegisterProjectNamespace(context.Background(), &namespaceRequest)
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
			execUnit1 := new(mock.BasePlugin)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:  taskName,
				Image: "random-image",
			}, nil)
			defer execUnit1.AssertExpectations(t)

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			jobSpec := models.JobSpec{
				Name: jobName,
				Task: models.JobSpecTask{
					Unit: &models.Plugin{
						Base: execUnit1,
					},
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

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSvc := new(mock.JobService)
			jobSvc.On("Create", ctx, jobSpec, namespaceSpec).Return(nil)
			jobSvc.On("Check", ctx, namespaceSpec, []models.JobSpec{jobSpec}, mock2.Anything).Return(nil)
			jobSvc.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobSvc,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			jobProto, _ := adapter.ToJobProto(jobSpec)
			request := pb.CreateJobSpecificationRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceSpec.Name,
				Spec:          jobProto,
			}
			resp, err := runtimeServiceServer.CreateJobSpecification(context.Background(), &request)
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
			adapter := v1.NewAdapter(nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Save", ctx, sec).Return(nil)
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobService, nil, nil,
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
			resp, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
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
			adapter := v1.NewAdapter(nil, nil)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			sec := models.ProjectSecretItem{
				Name:  "hello",
				Value: "world",
			}

			projectSecretRepository := new(mock.ProjectSecretRepository)
			projectSecretRepository.On("Save", ctx, sec).Return(errors.New("random error"))
			defer projectSecretRepository.AssertExpectations(t)

			projectSecretRepoFactory := new(mock.ProjectSecretRepoFactory)
			projectSecretRepoFactory.On("New", projectSpec).Return(projectSecretRepository)
			defer projectSecretRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobService, nil, nil,
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
			resp, err := runtimeServiceServer.RegisterSecret(context.Background(), &secretRequest)
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

			execUnit1 := new(mock.BasePlugin)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: taskName,
			}, nil)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: &models.Plugin{
							Base: execUnit1,
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
						}),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFactory.AssertExpectations(t)

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("Create", ctx, mock2.Anything, namespaceSpec).Return(nil)
			jobService.On("KeepOnly", ctx, namespaceSpec, mock2.Anything, mock2.Anything).Return(nil)
			jobService.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobService.AssertExpectations(t)

			grpcRespStream := new(mock.RuntimeService_DeployJobSpecificationServer)
			grpcRespStream.On("Context").Return(context.Background())
			defer grpcRespStream.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
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
			deployRequest := pb.DeployJobSpecificationRequest{ProjectName: projectName, Jobs: jobSpecsAdapted, NamespaceName: namespaceSpec.Name}
			err := runtimeServiceServer.DeployJobSpecification(&deployRequest, grpcRespStream)
			assert.Nil(t, err)
		})
	})

	t.Run("GetJobSpecification", func(t *testing.T) {
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

			execUnit1 := new(mock.BasePlugin)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: taskName,
			}, nil)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: &models.Plugin{
							Base: execUnit1,
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
						}),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			jobSpecAdapted, _ := adapter.ToJobProto(jobSpecs[0])
			deployRequest := pb.GetJobSpecificationRequest{ProjectName: projectName, JobName: jobSpecs[0].Name, NamespaceName: namespaceSpec.Name}
			jobSpecResp, err := runtimeServiceServer.GetJobSpecification(context.Background(), &deployRequest)
			assert.Nil(t, err)
			assert.Equal(t, jobSpecAdapted, jobSpecResp.Spec)
		})
	})
	t.Run("GetJobTask", func(t *testing.T) {
		t.Run("should read a job spec task details", func(t *testing.T) {
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

			execUnit1 := new(mock.DependencyResolverMod)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:        taskName,
				Description: "plugin description",
				Image:       "gcr.io/example/example",
			}, nil)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
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
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			jobService.On("GetTaskDependencies", ctx, namespaceSpec, jobSpecs[0]).Return(models.JobSpecTaskDestination{
				Destination: "project.dataset.table",
				Type:        "bq",
			}, models.JobSpecTaskDependencies([]string{"bq://project.dataset.table"}), nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			taskSpecExpected := &pb.JobTask{
				Name:        taskName,
				Description: "plugin description",
				Image:       "gcr.io/example/example",
				Destination: &pb.JobTask_Destination{
					Destination: "project.dataset.table",
					Type:        "bq",
				},
				Dependencies: []*pb.JobTask_Dependency{
					{
						Dependency: "bq://project.dataset.table",
					},
				},
			}
			jobTaskRequest := &pb.GetJobTaskRequest{ProjectName: projectName, JobName: jobSpecs[0].Name, NamespaceName: namespaceSpec.Name}
			jobTaskResp, err := runtimeServiceServer.GetJobTask(ctx, jobTaskRequest)
			assert.Nil(t, err)
			assert.Equal(t, taskSpecExpected, jobTaskResp.Task)
		})
		t.Run("task without dependency mod should skip destination and dependency fields", func(t *testing.T) {
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

			execUnit1 := new(mock.BasePlugin)
			execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:        taskName,
				Description: "plugin description",
				Image:       "gcr.io/example/example",
			}, nil)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: &models.Plugin{
							Base: execUnit1,
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
						}),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			taskSpecExpected := &pb.JobTask{
				Name:         taskName,
				Description:  "plugin description",
				Image:        "gcr.io/example/example",
				Destination:  nil,
				Dependencies: nil,
			}
			jobTaskRequest := &pb.GetJobTaskRequest{ProjectName: projectName, JobName: jobSpecs[0].Name, NamespaceName: namespaceSpec.Name}
			jobTaskResp, err := runtimeServiceServer.GetJobTask(ctx, jobTaskRequest)
			assert.Nil(t, err)
			assert.Equal(t, taskSpecExpected, jobTaskResp.Task)
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
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetAll", ctx).Return([]models.NamespaceSpec{namespaceSpec}, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
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
			resp, err := runtimeServiceServer.ListProjectNamespaces(ctx, &request)
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

			execUnit1 := new(mock.BasePlugin)
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: &models.Plugin{
							Base: execUnit1,
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
						}),
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobSpec := jobSpecs[0]

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			jobService.On("Delete", mock2.Anything, namespaceSpec, jobSpec).Return(nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			deployRequest := pb.DeleteJobSpecificationRequest{ProjectName: projectName, JobName: jobSpec.Name, NamespaceName: namespaceSpec.Name}
			resp, err := runtimeServiceServer.DeleteJobSpecification(ctx, &deployRequest)
			assert.Nil(t, err)
			assert.Equal(t, "job a-data-job has been deleted", resp.GetMessage())
		})
	})

	t.Run("JobStatus", func(t *testing.T) {
		t.Run("should return all job status via scheduler if valid inputs", func(t *testing.T) {
			Version := "1.0.0"

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "a-data-project",
			}

			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "game_jam",
				ProjectSpec: projectSpec,
			}

			jobSpec := models.JobSpec{
				Name: "transform-tables",
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			jobStatuses := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
					State:       "failed",
				},
				{
					ScheduledAt: time.Date(2020, 11, 10, 0, 0, 0, 0, time.UTC),
					State:       "success",
				},
			}
			scheduler := new(mock.Scheduler)
			scheduler.On("GetJobStatus", ctx, projectSpec, jobSpec.Name).Return(jobStatuses, nil)
			defer scheduler.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService, nil, nil,
				projectRepoFactory,
				nil,
				nil,
				adapter,
				nil,
				nil,
				scheduler,
			)

			req := &pb.JobStatusRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
			}
			resp, err := runtimeServiceServer.JobStatus(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobStatuses), len(resp.Statuses))
			for _, expectedStatus := range jobStatuses {
				var found bool
				for _, respVal := range resp.Statuses {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.State.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job status %v", expectedStatus))
				}
			}
		})
	})

	t.Run("RegisterJobEvent", func(t *testing.T) {
		t.Run("should register the event if valid inputs", func(t *testing.T) {
			Version := "1.0.0"

			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "a-data-project",
			}

			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "game_jam",
				ProjectSpec: projectSpec,
			}

			jobSpecs := []models.JobSpec{
				{
					Name: "transform-tables",
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			eventSvc := new(mock.EventService)
			eventSvc.On("Register", ctx, namespaceSpec, jobSpecs[0], models.JobEvent{
				Type:  models.JobEventTypeFailure,
				Value: eventValues.GetFields(),
			}).Return(nil)
			defer eventSvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService, eventSvc, nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectSpec.Name,
				JobName:       jobSpecs[0].Name,
				NamespaceName: namespaceSpec.Name,
				Event: &pb.JobEvent{
					Type:  pb.JobEvent_TYPE_FAILURE,
					Value: eventValues,
				},
			}
			_, err := runtimeServiceServer.RegisterJobEvent(ctx, req)
			assert.Nil(t, err)
		})
	})

	t.Run("GetWindow", func(t *testing.T) {
		t.Run("should return the correct window date range", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				nil, nil, nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			scheduledAtTimestamp := timestamppb.New(scheduledAt)
			req := pb.GetWindowRequest{
				ScheduledAt: scheduledAtTimestamp,
				Size:        "24h",
				Offset:      "24h",
				TruncateTo:  "d",
			}
			resp, err := runtimeServiceServer.GetWindow(ctx, &req)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", resp.GetStart().AsTime().Format(time.RFC3339))
			assert.Equal(t, "2020-11-12T00:00:00Z", resp.GetEnd().AsTime().Format(time.RFC3339))
		})
		t.Run("should return error if any of the required fields in request is missing", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				nil, nil,
				nil, nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			scheduledAtTimestamp := timestamppb.New(scheduledAt)
			req := pb.GetWindowRequest{
				ScheduledAt: scheduledAtTimestamp,
				Size:        "",
				Offset:      "24h",
				TruncateTo:  "d",
			}
			_, err := runtimeServiceServer.GetWindow(ctx, &req)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = window size, offset and truncate_to must be provided", err.Error())
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
				NamespaceName: namespaceSpec.Name,
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("CreateResource", ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(nil, dsRepo),
				nil,
				nil,
				nil,
			)

			resp, err := runtimeServiceServer.CreateResource(ctx, &req)
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
				NamespaceName: namespaceSpec.Name,
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			resourceSvc.On("UpdateResource", ctx, namespaceSpec, []models.ResourceSpec{resourceSpec}, nil).Return(nil)
			defer resourceSvc.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				v1.NewAdapter(nil, dsRepo),
				nil,
				nil,
				nil,
			)

			resp, err := runtimeServiceServer.UpdateResource(ctx, &req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.GetSuccess())
		})
	})

	t.Run("ReplayDryRun", func(t *testing.T) {
		projectName := "a-data-project"
		jobName := "a-data-job"
		timeLayout := "2006-01-02"
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
		t.Run("should do replay dry run successfully", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			dagNode := tree.NewTreeNode(jobSpec)
			dagNode.Runs.Add(time.Date(2020, 11, 25, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 26, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 27, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 28, 2, 0, 0, 0, time.UTC))
			replayPlan := models.ReplayPlan{ExecutionTree: dagNode}

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", ctx, replayWorkerRequest).Return(replayPlan, nil)
			defer jobService.AssertExpectations(t)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.Background(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, true, replayResponse.Success)
			expectedReplayResponse, err := adapter.ToReplayExecutionTreeNode(dagNode)
			assert.Nil(t, err)
			assert.Equal(t, expectedReplayResponse.JobName, replayResponse.ExecutionTree.JobName)
			assert.Equal(t, expectedReplayResponse.Dependents, replayResponse.ExecutionTree.Dependents)
			assert.Equal(t, expectedReplayResponse.Runs, replayResponse.ExecutionTree.Runs)
		})
		t.Run("should do replay dry run including only allowed namespace successfully", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			dagNode := tree.NewTreeNode(jobSpec)
			dagNode.Runs.Add(time.Date(2020, 11, 25, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 26, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 27, 2, 0, 0, 0, time.UTC))
			dagNode.Runs.Add(time.Date(2020, 11, 28, 2, 0, 0, 0, time.UTC))
			replayPlan := models.ReplayPlan{ExecutionTree: dagNode}

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", ctx, replayWorkerRequest).Return(replayPlan, nil)
			defer jobService.AssertExpectations(t)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(ctx, &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, true, replayResponse.Success)
			expectedReplayResponse, err := adapter.ToReplayExecutionTreeNode(dagNode)
			assert.Nil(t, err)
			assert.Equal(t, expectedReplayResponse.JobName, replayResponse.ExecutionTree.JobName)
			assert.Equal(t, expectedReplayResponse.Dependents, replayResponse.ExecutionTree.Dependents)
			assert.Equal(t, expectedReplayResponse.Runs, replayResponse.ExecutionTree.Runs)
		})
		t.Run("should failed when replay request is invalid", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 24, 0, 0, 0, 0, time.UTC)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			defer jobService.AssertExpectations(t)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceSpec.Name,
				JobName:       jobName,
				StartDate:     startDate.Format(timeLayout),
				EndDate:       endDate.Format(timeLayout),
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when unable to prepare the job specs", func(t *testing.T) {
			startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
			endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("ReplayDryRun", ctx, replayWorkerRequest).Return(models.ReplayPlan{}, errors.New("populating jobs spec failed"))
			defer jobService.AssertExpectations(t)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayDryRunRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.ReplayDryRun(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Nil(t, replayResponse)
		})
	})

	t.Run("Replay", func(t *testing.T) {
		projectName := "a-data-project"
		jobName := "a-data-job"
		timeLayout := "2006-01-02"
		startDate := time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)
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
		t.Run("should do replay successfully", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			randomUUID := uuid.Must(uuid.NewRandom())

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{ID: randomUUID}, nil)
			defer jobService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, randomUUID.String(), replayResponse.Id)
		})
		t.Run("should do replay including only allowed namespace successfully", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			randomUUID := uuid.Must(uuid.NewRandom())

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{ID: randomUUID}, nil)
			defer jobService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.Nil(t, err)
			assert.Equal(t, randomUUID.String(), replayResponse.Id)
		})
		t.Run("should failed when replay request is invalid", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New("Namespace not found"))
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when replay process is failed", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			errMessage := "internal error"

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{}, errors.New(errMessage))
			defer jobService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when project is not found", func(t *testing.T) {
			errMessage := "project not found"
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(models.ProjectSpec{}, errors.New(errMessage))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil,
				nil,
				nil,
				projectRepoFactory,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)
			replayRequest := pb.ReplayRequest{
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when job is not found in the namespace", func(t *testing.T) {
			errMessage := "job not found in namespace"

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(models.JobSpec{}, errors.New(errMessage))
			defer jobService.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when replay validation is failed", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{}, job.ErrConflictedJobRun)
			defer jobService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), job.ErrConflictedJobRun.Error())
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Nil(t, replayResponse)
		})
		t.Run("should failed when request queue is full", func(t *testing.T) {
			replayWorkerRequest := models.ReplayRequest{
				Job:                         jobSpec,
				Start:                       startDate,
				End:                         endDate,
				Project:                     projectSpec,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobName, namespaceSpec).Return(jobSpec, nil)
			jobService.On("Replay", ctx, replayWorkerRequest).Return(models.ReplayResult{}, job.ErrRequestQueueFull)
			defer jobService.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			defer namespaceRepoFact.AssertExpectations(t)
			adapter := v1.NewAdapter(nil, nil)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService,
				nil,
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
				ProjectName:                 projectName,
				NamespaceName:               namespaceSpec.Name,
				JobName:                     jobName,
				StartDate:                   startDate.Format(timeLayout),
				EndDate:                     endDate.Format(timeLayout),
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			replayResponse, err := runtimeServiceServer.Replay(context.TODO(), &replayRequest)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), job.ErrRequestQueueFull.Error())
			assert.Equal(t, codes.Unavailable, status.Code(err))
			assert.Nil(t, replayResponse)
		})
	})

	t.Run("GetReplayStatus", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}
		reqUUID := uuid.Must(uuid.NewRandom())
		replayRequest := models.ReplayRequest{
			ID:      reqUUID,
			Project: projectSpec,
		}

		t.Run("should get status of each jobs and runs of a replay", func(t *testing.T) {
			jobName := "a-data-job"
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

			jobStatusList := []models.JobStatus{
				{
					ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
					State:       models.RunStateRunning,
				},
				{
					ScheduledAt: time.Date(2020, 11, 12, 0, 0, 0, 0, time.UTC),
					State:       models.RunStateRunning,
				},
			}

			dagNode := tree.NewTreeNode(jobSpec)
			dagNode.Runs = set.NewTreeSetWith(job.TimeOfJobStatusComparator)
			dagNode.Runs.Add(jobStatusList[0])
			dagNode.Runs.Add(jobStatusList[1])
			replayState := models.ReplayState{
				Status: models.ReplayStatusReplayed,
				Node:   dagNode,
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayStatus", context.TODO(), replayRequest).Return(replayState, nil)

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectRepoFactory,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			expectedReplayStatusNodeResponse, err := adapter.ToReplayStatusTreeNode(replayState.Node)
			assert.Nil(t, err)

			replayRequestPb := pb.GetReplayStatusRequest{
				Id:          reqUUID.String(),
				ProjectName: projectName,
			}
			replayStatusResponse, err := runtimeServiceServer.GetReplayStatus(ctx, &replayRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, models.ReplayStatusReplayed, replayStatusResponse.State)
			assert.Equal(t, expectedReplayStatusNodeResponse.Runs, replayStatusResponse.Response.Runs)
		})
		t.Run("should failed when unable to get status of a replay", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			errMessage := "internal error"
			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayStatus", context.TODO(), replayRequest).Return(models.ReplayState{}, errors.New(errMessage))

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectRepoFactory,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			replayRequestPb := pb.GetReplayStatusRequest{
				Id:          reqUUID.String(),
				ProjectName: projectName,
			}
			replayStatusResponse, err := runtimeServiceServer.GetReplayStatus(ctx, &replayRequestPb)

			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayStatusResponse)
		})
	})

	t.Run("ListReplays", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}

		t.Run("should get list of replay for a project", func(t *testing.T) {
			jobName := "a-data-job"
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

			replaySpecs := []models.ReplaySpec{
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       jobSpec,
					StartDate: time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC),
					EndDate:   time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC),
					Status:    models.ReplayStatusReplayed,
					CreatedAt: time.Date(2021, 8, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					ID:        uuid.Must(uuid.NewRandom()),
					Job:       jobSpec,
					StartDate: time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC),
					EndDate:   time.Date(2020, 12, 28, 0, 0, 0, 0, time.UTC),
					Status:    models.ReplayStatusInProgress,
					CreatedAt: time.Date(2021, 8, 2, 0, 0, 0, 0, time.UTC),
				},
			}
			expectedReplayList := &pb.ListReplaysResponse{
				ReplayList: []*pb.ReplaySpec{
					{
						Id:        replaySpecs[0].ID.String(),
						JobName:   jobSpec.Name,
						StartDate: timestamppb.New(time.Date(2020, 11, 25, 0, 0, 0, 0, time.UTC)),
						EndDate:   timestamppb.New(time.Date(2020, 11, 28, 0, 0, 0, 0, time.UTC)),
						State:     models.ReplayStatusReplayed,
						CreatedAt: timestamppb.New(time.Date(2021, 8, 1, 0, 0, 0, 0, time.UTC)),
					},
					{
						Id:        replaySpecs[1].ID.String(),
						JobName:   jobSpec.Name,
						StartDate: timestamppb.New(time.Date(2020, 12, 25, 0, 0, 0, 0, time.UTC)),
						EndDate:   timestamppb.New(time.Date(2020, 12, 28, 0, 0, 0, 0, time.UTC)),
						State:     models.ReplayStatusInProgress,
						CreatedAt: timestamppb.New(time.Date(2021, 8, 2, 0, 0, 0, 0, time.UTC)),
					},
				},
			}

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayList", ctx, projectSpec.ID).Return(replaySpecs, nil)

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectRepoFactory,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			replayRequestPb := pb.ListReplaysRequest{
				ProjectName: projectName,
			}
			replayStatusResponse, err := runtimeServiceServer.ListReplays(ctx, &replayRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, expectedReplayList, replayStatusResponse)
		})
		t.Run("should failed when unable to get status of a replay", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			errMessage := "internal error"
			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetReplayList", ctx, projectSpec.ID).Return([]models.ReplaySpec{}, errors.New(errMessage))

			adapter := v1.NewAdapter(nil, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				nil,
				projectRepoFactory,
				nil,
				nil,
				adapter,
				nil,
				nil,
				nil,
			)

			replayRequestPb := pb.ListReplaysRequest{
				ProjectName: projectName,
			}
			replayListResponse, err := runtimeServiceServer.ListReplays(ctx, &replayRequestPb)

			assert.Contains(t, err.Error(), errMessage)
			assert.Nil(t, replayListResponse)
		})
	})

	t.Run("BackupDryRun", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		resourceName := "a-data-project:dataset.table"
		resourceUrn := "bigquery://a-data-project:dataset.table"

		t.Run("should return list of resources for backup ignoring downstream", func(t *testing.T) {
			jobName := "a-data-job"
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
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}

			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			backupRequest := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				DryRun:       true,
			}
			backupPlan := models.BackupPlan{Resources: []string{resourceName}}
			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest, []models.JobSpec{jobSpec, jobSpecDownstreams[0]}).Return(backupPlan, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, []string{resourceName}, backupResponse.ResourceName)
		})
		t.Run("should return list of resources for backup with downstream", func(t *testing.T) {
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}
			allowedDownstream := []string{models.AllNamespace}

			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceDownstream1Urn := "bigquery://a-data-project:dataset.downstream1"
			resourceDownstream2Urn := "bigquery://a-data-project:dataset.downstream2"

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", context.Background(), namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			backupRequest := models.BackupRequest{
				ResourceName:                resourceName,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupPlan := models.BackupPlan{
				Resources: []string{
					resourceUrn,
					resourceDownstream1Urn,
					resourceDownstream2Urn,
				},
			}
			resourceSvc.On("BackupResourceDryRun", context.Background(), backupRequest,
				[]models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupPlan, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: []string{models.AllNamespace},
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(context.Background(), &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, []string{resourceUrn, resourceDownstream1Urn, resourceDownstream2Urn}, backupResponse.ResourceName)
		})
		t.Run("should return list of resources for backup with same namespace downstream", func(t *testing.T) {
			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}

			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceDownstream1Urn := "bigquery://a-data-project:dataset.downstream1"
			resourceDownstream2Urn := "bigquery://a-data-project:dataset.downstream2"

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			backupRequest := models.BackupRequest{
				ResourceName:                resourceName,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupPlan := models.BackupPlan{
				Resources: []string{
					resourceUrn,
					resourceDownstream1Urn,
					resourceDownstream2Urn,
				},
			}
			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest,
				[]models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupPlan, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, []string{resourceUrn, resourceDownstream1Urn, resourceDownstream2Urn}, backupResponse.ResourceName)
		})
		t.Run("should return error when project is not found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			errorMsg := "unable to fetch project"
			projectRepository.On("GetByName", ctx, projectName).Return(models.ProjectSpec{}, errors.New(errorMsg))

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when namespace is not found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			errorMsg := "unable to get namespace"
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil,
				nil,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to read resource", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			errorMsg := "unable to read resource"
			resourceSvc.On("ReadResource", ctx, namespaceSpec,
				models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			errorMsg := "unable to get jobspec"
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(models.JobSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec downstream", func(t *testing.T) {
			jobName := "a-data-job"
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
			allowedDownstream := []string{models.AllNamespace}

			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			errorMsg := "unable to get jobspec downstream"
			jobService.On("GetDownstream", ctx, projectSpec, jobSpec.Name).Return([]models.JobSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to do backup dry run", func(t *testing.T) {
			jobName := "a-data-job"
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
			allowedDownstream := []string{models.AllNamespace}

			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)
			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return([]models.JobSpec{}, nil)

			backupRequest := models.BackupRequest{
				ResourceName:                resourceName,
				Project:                     projectSpec,
				Namespace:                   namespaceSpec,
				DryRun:                      true,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			errorMsg := "unable to get jobspec"

			resourceSvc.On("BackupResourceDryRun", ctx, backupRequest, []models.JobSpec{jobSpec}).
				Return(models.BackupPlan{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)

			backupRequestPb := pb.BackupDryRunRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupResponse, err := runtimeServiceServer.BackupDryRun(ctx, &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
	})

	t.Run("Backup", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		resourceName := "a-data-project:dataset.table"
		resourceUrn := "datastore://a-data-project:dataset.table"
		backupUrn := "datastore://a-data-project:optimus_backup.table_backup"

		t.Run("should able to do backup ignoring downstream", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobName := "a-data-job"
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
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
			}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun: false,
			}
			backupResponsePb := &pb.CreateBackupResponse{
				Urn: []string{backupUrn},
			}
			backupResult := models.BackupResult{
				Resources: []string{backupUrn},
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			resourceSvc.On("BackupResource", ctx, backupReq, []models.JobSpec{jobSpec}).Return(backupResult, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(ctx, &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, backupResponsePb, backupResponse)
		})
		t.Run("should return list of resources for backup with downstream", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}
			allowedDownstream := []string{models.AllNamespace}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupDownstream1Urn := "datastore://a-data-project:optimus_backup.downstream1"
			backupDownstream2Urn := "datastore://a-data-project:optimus_backup.downstream2"
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun:                      false,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			urns := []string{backupUrn, backupDownstream1Urn, backupDownstream2Urn}
			backupResult := models.BackupResult{
				Resources: urns,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSvc.On("ReadResource", context.Background(), namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			resourceSvc.On("BackupResource", context.Background(), backupReq, []models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupResult, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, backupResult.Resources, backupResponse.Urn)
			assert.Equal(t, backupResult.IgnoredResources, backupResponse.IgnoredResources)
		})
		t.Run("should return list of resources for backup with downstream with only allowed namespace", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: "a-data-job",
			}
			jobSpecDownstreams := []models.JobSpec{
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "b-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
				{
					ID:   uuid.Must(uuid.NewRandom()),
					Name: "c-data-job",
					Task: models.JobSpecTask{
						Config: models.JobSpecConfigs{
							{
								Name:  "do",
								Value: "this",
							},
						},
					},
				},
			}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupDownstream1Urn := "datastore://a-data-project:optimus_backup.downstream1"
			backupDownstream2Urn := "datastore://a-data-project:optimus_backup.downstream2"
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun:                      false,
				AllowedDownstreamNamespaces: []string{namespaceSpec.Name},
			}
			urns := []string{backupUrn, backupDownstream1Urn, backupDownstream2Urn}
			backupResult := models.BackupResult{
				Resources: urns,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)

			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			resourceSvc.On("ReadResource", ctx, namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)

			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return(jobSpecDownstreams, nil)

			resourceSvc.On("BackupResource", context.Background(), backupReq, []models.JobSpec{jobSpec, jobSpecDownstreams[0], jobSpecDownstreams[1]}).Return(backupResult, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Nil(t, err)
			assert.Equal(t, backupResult.Resources, backupResponse.Urn)
			assert.Equal(t, backupResult.IgnoredResources, backupResponse.IgnoredResources)
		})
		t.Run("should return error when project is not found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
			}

			projectRepoFactory.On("New").Return(projectRepository)
			errorMsg := "unable to fetch project"
			projectRepository.On("GetByName", ctx, projectName).Return(models.ProjectSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when namespace is not found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)
			errorMsg := "unable to get namespace"
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(models.NamespaceSpec{}, errors.New(errorMsg))
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil,
				nil,
				nil,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to read resource", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			errorMsg := "unable to read resource"
			resourceSvc.On("ReadResource", context.Background(), namespaceSpec,
				models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			resourceSvc.On("ReadResource", context.Background(), namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			errorMsg := "unable to get jobspec"
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(models.JobSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get jobSpec downstream", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobName := "a-data-job"
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
			allowedDownstream := []string{models.AllNamespace}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:                 projectName,
				DatastoreName:               models.DestinationTypeBigquery.String(),
				ResourceName:                resourceName,
				NamespaceName:               namespaceSpec.Name,
				AllowedDownstreamNamespaces: allowedDownstream,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			resourceSvc.On("ReadResource", context.Background(), namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			errorMsg := "unable to get jobspec downstream"
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return([]models.JobSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to do backup", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			namespaceRepository := new(mock.NamespaceRepository)
			defer namespaceRepository.AssertExpectations(t)

			namespaceRepoFact := new(mock.NamespaceRepoFactory)
			defer namespaceRepoFact.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			jobName := "a-data-job"
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
			allowedDownstream := []string{models.AllNamespace}
			backupReq := models.BackupRequest{
				ResourceName: resourceName,
				Project:      projectSpec,
				Namespace:    namespaceSpec,
				Config: map[string]string{
					"TTL": "30",
				},
				DryRun:                      false,
				AllowedDownstreamNamespaces: allowedDownstream,
			}
			resourceSpec := models.ResourceSpec{
				Name: resourceName,
				URN:  resourceUrn,
			}
			backupRequestPb := pb.CreateBackupRequest{
				ProjectName:   projectName,
				DatastoreName: models.DestinationTypeBigquery.String(),
				ResourceName:  resourceName,
				NamespaceName: namespaceSpec.Name,
				Config: map[string]string{
					"TTL": "30",
				},
				AllowedDownstreamNamespaces: allowedDownstream,
			}

			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			projectRepoFactory.On("New").Return(projectRepository)
			namespaceRepository.On("GetByName", ctx, namespaceSpec.Name).Return(namespaceSpec, nil)
			namespaceRepoFact.On("New", projectSpec).Return(namespaceRepository)
			resourceSvc.On("ReadResource", context.Background(), namespaceSpec, models.DestinationTypeBigquery.String(), resourceName).Return(resourceSpec, nil)
			jobService.On("GetByDestination", projectSpec, resourceUrn).Return(jobSpec, nil)
			jobService.On("GetDownstream", context.Background(), projectSpec, jobSpec.Name).Return([]models.JobSpec{}, nil)
			errorMsg := "unable to get jobspec"
			resourceSvc.On("BackupResource", context.Background(), backupReq, []models.JobSpec{jobSpec}).Return(models.BackupResult{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				jobService, nil,
				resourceSvc,
				projectRepoFactory,
				namespaceRepoFact,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.CreateBackup(context.Background(), &backupRequestPb)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
	})

	t.Run("ListBackups", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}
		datastoreName := models.DestinationTypeBigquery.String()
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		listBackupsReq := pb.ListBackupsRequest{
			ProjectName:   projectName,
			DatastoreName: datastoreName,
			NamespaceName: namespaceSpec.Name,
		}
		backupSpecs := []models.BackupSpec{
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
			},
			{
				ID:        uuid.Must(uuid.NewRandom()),
				CreatedAt: time.Now().Add(time.Hour * 24 * -50),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
			},
		}
		t.Run("should return list of backups", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupResultPb := &pb.ListBackupsResponse{
				Backups: []*pb.BackupSpec{
					{
						Id:           backupSpecs[0].ID.String(),
						ResourceName: backupSpecs[0].Resource.Name,
						CreatedAt:    timestamppb.New(backupSpecs[0].CreatedAt),
						Description:  backupSpecs[0].Description,
					},
					{
						Id:           backupSpecs[1].ID.String(),
						ResourceName: backupSpecs[1].Resource.Name,
						CreatedAt:    timestamppb.New(backupSpecs[1].CreatedAt),
						Description:  backupSpecs[1].Description,
					},
				},
			}

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			resourceSvc.On("ListResourceBackups", ctx, projectSpec, datastoreName).Return(backupSpecs, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.ListBackups(context.Background(), &listBackupsReq)

			assert.Nil(t, err)
			assert.Equal(t, backupResultPb, backupResponse)
		})
		t.Run("should return error when unable to get project spec", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			projectRepoFactory.On("New").Return(projectRepository)
			errorMsg := "unable to get project spec"
			projectRepository.On("GetByName", ctx, projectName).Return(models.ProjectSpec{},
				errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.ListBackups(context.Background(), &listBackupsReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get list of backups", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			errorMsg := "unable to get list of backups"
			resourceSvc.On("ListResourceBackups", ctx, projectSpec, datastoreName).Return([]models.BackupSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.ListBackups(context.Background(), &listBackupsReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
	})

	t.Run("GetBackup", func(t *testing.T) {
		projectName := "a-data-project"
		projectSpec := models.ProjectSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: projectName,
		}
		datastoreName := models.DestinationTypeBigquery.String()
		namespaceSpec := models.NamespaceSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "dev-test-namespace-1",
			Config: map[string]string{
				"bucket": "gs://some_folder",
			},
			ProjectSpec: projectSpec,
		}
		backupID := uuid.Must(uuid.NewRandom())
		getBackupDetailReq := pb.GetBackupRequest{
			ProjectName:   projectName,
			DatastoreName: datastoreName,
			NamespaceName: namespaceSpec.Name,
			Id:            backupID.String(),
		}
		backupSpec := models.BackupSpec{
			ID:        backupID,
			CreatedAt: time.Now().Add(time.Hour * 24 * -30),
			Resource: models.ResourceSpec{
				Name: "sample resource",
			},
			Description: "backup purpose",
		}
		t.Run("should return backup detail", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			backupResultPb := &pb.GetBackupResponse{
				Spec: &pb.BackupSpec{
					Id:           backupSpec.ID.String(),
					ResourceName: backupSpec.Resource.Name,
					CreatedAt:    timestamppb.New(backupSpec.CreatedAt),
					Description:  backupSpec.Description,
				},
			}

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(backupSpec, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.GetBackup(context.Background(), &getBackupDetailReq)

			assert.Nil(t, err)
			assert.Equal(t, backupResultPb, backupResponse)
		})
		t.Run("should return error when unable to get project spec", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			projectRepoFactory.On("New").Return(projectRepository)
			errorMsg := "unable to get project spec"
			projectRepository.On("GetByName", ctx, projectName).Return(models.ProjectSpec{},
				errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.GetBackup(context.Background(), &getBackupDetailReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when unable to get backup detail", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			errorMsg := "unable to get backup detail"
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(models.BackupSpec{}, errors.New(errorMsg))

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.GetBackup(context.Background(), &getBackupDetailReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when backup is not found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(models.BackupSpec{}, store.ErrResourceNotFound)

			errorMsg := fmt.Sprintf("backup with ID %s not found", backupID)
			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.GetBackup(context.Background(), &getBackupDetailReq)

			assert.Contains(t, err.Error(), errorMsg)
			assert.Nil(t, backupResponse)
		})
		t.Run("should return error when backup URN is not found", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			invalidBackupSpec := models.BackupSpec{
				ID:        backupID,
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
				Result: map[string]interface{}{
					"sample_table": map[string]interface{}{
						"other_key": "other_value",
					},
				},
			}

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(invalidBackupSpec, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.GetBackup(context.Background(), &getBackupDetailReq)

			assert.Nil(t, backupResponse)
			assert.Contains(t, err.Error(), "URN is not found in backup result")
		})
		t.Run("should return error when backup URN is invalid", func(t *testing.T) {
			projectRepository := new(mock.ProjectRepository)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			defer projectRepoFactory.AssertExpectations(t)

			resourceSvc := new(mock.DatastoreService)
			defer resourceSvc.AssertExpectations(t)

			invalidBackupSpec := models.BackupSpec{
				ID:        backupID,
				CreatedAt: time.Now().Add(time.Hour * 24 * -30),
				Resource: models.ResourceSpec{
					Name: "sample resource",
				},
				Description: "backup purpose",
				Result: map[string]interface{}{
					"sample_table": map[string]interface{}{
						models.BackupSpecKeyURN: 0,
					},
				},
			}

			projectRepoFactory.On("New").Return(projectRepository)
			projectRepository.On("GetByName", ctx, projectName).Return(projectSpec, nil)
			resourceSvc.On("GetResourceBackup", ctx, projectSpec, datastoreName, backupID).Return(invalidBackupSpec, nil)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"Version",
				nil, nil,
				resourceSvc,
				projectRepoFactory,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			backupResponse, err := runtimeServiceServer.GetBackup(context.Background(), &getBackupDetailReq)

			assert.Nil(t, backupResponse)
			assert.Contains(t, err.Error(), "invalid backup URN")
		})
	})
}
