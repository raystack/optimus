package v1beta1_test

import (
	"context"
	"testing"
	"time"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"

	"github.com/google/uuid"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
)

func TestJobSpecificationOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

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

			jobSpecRepository := new(mock.JobSpecRepository)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			defer jobSpecRepoFactory.AssertExpectations(t)

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			nsService := new(mock.NamespaceService)
			nsService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer nsService.AssertExpectations(t)

			projectJobSpecRepository := new(mock.ProjectJobSpecRepository)
			defer projectJobSpecRepository.AssertExpectations(t)

			projectJobSpecRepoFactory := new(mock.ProjectJobSpecRepoFactory)
			defer projectJobSpecRepoFactory.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("Create", ctx, mock2.Anything, namespaceSpec).Return(nil)
			jobService.On("KeepOnly", ctx, namespaceSpec, mock2.Anything, mock2.Anything).Return(nil)
			jobService.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobService.AssertExpectations(t)

			grpcRespStream := new(mock.DeployJobSpecificationServer)
			grpcRespStream.On("Context").Return(context.Background())
			defer grpcRespStream.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				nil,
				nsService,
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
			// deployRequest := pb.DeployJobSpecificationRequest{ProjectName: projectName, Jobs: jobSpecsAdapted, NamespaceName: namespaceSpec.Name}
			err := runtimeServiceServer.DeployJobSpecification(grpcRespStream)
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

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService,
				nil, nil,
				nil,
				namespaceService,
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

			jobSvc := new(mock.JobService)
			jobSvc.On("Create", ctx, jobSpec, namespaceSpec).Return(nil)
			jobSvc.On("Check", ctx, namespaceSpec, []models.JobSpec{jobSpec}, mock2.Anything).Return(nil)
			jobSvc.On("Sync", mock2.Anything, namespaceSpec, mock2.Anything).Return(nil)
			defer jobSvc.AssertExpectations(t)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobSvc,
				nil, nil,
				nil,
				namespaceService,
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

			pluginRepo := new(mock.SupportedPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&models.Plugin{
				Base: execUnit1,
			}, nil)
			adapter := v1.NewAdapter(pluginRepo, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

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
				nil,
				namespaceService,
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
}
