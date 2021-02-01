package v1_test

import (
	"context"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	v1 "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"testing"
	"time"
)

func TestRuntimeServiceServer(t *testing.T) {
	logger.Init("INFO")

	t.Run("Version", func(t *testing.T) {
		t.Run("should save specs and return with data", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
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

			execUnit1 := new(mock.ExecutionUnit)
			execUnit1.On("GetName").Return(taskName)
			defer execUnit1.AssertExpectations(t)

			jobSpec := models.JobSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: jobName,
				Task: models.JobSpecTask{
					Unit: execUnit1,
					Config: map[string]string{
						"do": "this",
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
						Name:  "EXECUTION_TIME",
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DSTART",
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DEND",
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
			defer jobService.AssertExpectations(t)

			jobSpecRepository := new(mock.JobSpecRepository)
			jobSpecRepository.On("GetByName", jobName).Return(jobSpec, nil)
			defer jobSpecRepository.AssertExpectations(t)

			jobSpecRepoFactory := new(mock.JobSpecRepoFactory)
			jobSpecRepoFactory.On("New", projectSpec).Return(jobSpecRepository)
			defer jobSpecRepoFactory.AssertExpectations(t)

			instanceService := new(mock.InstanceService)
			instanceService.On("Register", jobSpec, scheduledAt).Return(instanceSpec, nil)
			defer instanceService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				job.NewService(
					jobSpecRepoFactory,
					nil,
					nil,
					nil,
					nil,
				),
				projectRepoFactory,
				v1.NewAdapter(models.TaskRegistry),
				nil,
				instanceService,
			)

			versionRequest := pb.RegisterInstanceRequest{ProjectName: projectName, JobName: jobName, Type: "task", ScheduledAt: scheduledAtTimestamp}
			resp, err := runtimeServiceServer.RegisterInstance(context.TODO(), &versionRequest)
			assert.Nil(t, err)

			adapter := v1.NewAdapter(models.TaskRegistry)
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
					"bucket": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(models.TaskRegistry)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", projectSpec).Return(nil)
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				job.NewService(
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				projectRepoFactory,
				v1.NewAdapter(models.TaskRegistry),
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.TODO(), &projectRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectResponse{
				Succcess: true,
				Message:  "saved successfully",
			}, resp)
		})
		t.Run("should return error if saving to respository fails", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(models.TaskRegistry)

			projectRepository := new(mock.ProjectRepository)
			projectRepository.On("Save", projectSpec).Return(errors.New("a random error"))
			defer projectRepository.AssertExpectations(t)

			projectRepoFactory := new(mock.ProjectRepoFactory)
			projectRepoFactory.On("New").Return(projectRepository)
			defer projectRepoFactory.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				"someVersion1.0",
				job.NewService(
					nil,
					nil,
					nil,
					nil,
					nil,
				),
				projectRepoFactory,
				v1.NewAdapter(models.TaskRegistry),
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.TODO(), &projectRequest)
			assert.Equal(t, "rpc error: code = Internal desc = a random error: failed to save project a-data-project", err.Error())
			assert.Nil(t, resp)
		})
	})

	t.Run("DeploySpecification", func(t *testing.T) {
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

			execUnit1 := new(mock.ExecutionUnit)
			execUnit1.On("GetName").Return(taskName)
			execUnit1.On("GetImage").Return("imageName")
			defer execUnit1.AssertExpectations(t)

			jobSpecs := []models.JobSpec{
				{
					Name: jobName1,
					Task: models.JobSpecTask{
						Unit: execUnit1,
						Config: map[string]string{
							"do": "this",
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

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			models.TaskRegistry.Add(execUnit1)
			adapter := v1.NewAdapter(models.TaskRegistry)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				Version,
				job.NewService(
					jobSpecRepoFactory,
					nil,
					nil,
					nil,
					nil,
				),
				projectRepoFactory,
				adapter,
				nil,
				nil,
			)

			jobSpecsAdapted := []*pb.JobSpecification{}
			for _, jobSpec := range jobSpecs {
				jobSpecAdapted, _ := adapter.ToJobProto(jobSpec)
				jobSpecsAdapted = append(jobSpecsAdapted, jobSpecAdapted)
			}
			deployRequest := pb.DeploySpecificationRequest{ProjectName: projectName, Jobs: jobSpecsAdapted}
			err := runtimeServiceServer.DeploySpecification(&deployRequest, nil)
			assert.Equal(t, "rpc error: code = Internal desc = failed to save job: a-data-job: a random error: failed to save a-data-job", err.Error())
		})
	})
}
