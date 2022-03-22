package v1beta1_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/run"
	"github.com/odpf/optimus/utils"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestJobRunServiceServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()
	t.Run("RegisterInstance", func(t *testing.T) {
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
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobName, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			instanceService := new(mock.RunService)
			instanceService.On("GetScheduledRun", ctx, namespaceSpec, jobSpec, scheduledAt).Return(jobRun, nil)
			instanceService.On("Register", ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name).Return(instanceSpec, nil)
			instanceService.On("Compile", ctx, namespaceSpec, jobRun, instanceSpec).Return(
				&models.JobRunInput{
					ConfigMap: map[string]string{
						run.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					FileMap: map[string]string{
						"query.sql": "select * from 1",
					}}, nil)
			defer instanceService.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService,
				projectService, nil,
				v1.NewAdapter(nil, nil),
				instanceService,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{
				ProjectName:  projectName,
				JobName:      jobName,
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
				ScheduledAt:  scheduledAtTimestamp,
				InstanceName: instanceSpec.Name,
			}
			resp, err := JobRunServiceServer.RegisterInstance(context.Background(), &versionRequest)
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
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			instanceService := new(mock.RunService)
			instanceService.On("GetByID", ctx, jobRun.ID).Return(jobRun, namespaceSpec, nil)
			instanceService.On("Register", ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name).Return(instanceSpec, nil)
			instanceService.On("Compile", ctx, namespaceSpec, jobRun, instanceSpec).Return(
				&models.JobRunInput{
					ConfigMap: map[string]string{
						run.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						run.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					FileMap: map[string]string{
						"query.sql": "select * from 1",
					}}, nil)
			defer instanceService.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				nil,
				projectService,
				nil,
				v1.NewAdapter(nil, nil),
				instanceService,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{
				ProjectName:  projectName,
				JobrunId:     jobRun.ID.String(),
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
				InstanceName: instanceSpec.Name,
			}
			resp, err := JobRunServiceServer.RegisterInstance(context.Background(), &versionRequest)
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

	t.Run("GetJobTask", func(t *testing.T) {
		t.Run("should read a job spec task details", func(t *testing.T) {
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

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			nsService := new(mock.NamespaceService)
			nsService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).
				Return(namespaceSpec, nil)
			defer nsService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			jobService.On("GetTaskDependencies", ctx, namespaceSpec, jobSpecs[0]).Return(models.JobSpecTaskDestination{
				Destination: "project.dataset.table",
				Type:        "bq",
			}, models.JobSpecTaskDependencies([]string{"bq://project.dataset.table"}), nil)
			defer jobService.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService,
				nil,
				nsService, adapter,
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
			jobTaskResp, err := JobRunServiceServer.GetJobTask(ctx, jobTaskRequest)
			assert.Nil(t, err)
			assert.Equal(t, taskSpecExpected, jobTaskResp.Task)
		})
		t.Run("task without dependency mod should skip destination and dependency fields", func(t *testing.T) {
			//Version := "1.0.1"

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

			allTasksRepo := new(mock.SupportedPluginRepo)
			allTasksRepo.On("GetByName", taskName).Return(execUnit1, nil)
			adapter := v1.NewAdapter(allTasksRepo, nil)

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService,
				nil,
				namespaceService, adapter,
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
			jobTaskResp, err := JobRunServiceServer.GetJobTask(ctx, jobTaskRequest)
			assert.Nil(t, err)
			assert.Equal(t, taskSpecExpected, jobTaskResp.Task)
		})
	})

	t.Run("JobStatus", func(t *testing.T) {
		t.Run("should return all job status via scheduler if valid inputs", func(t *testing.T) {
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

			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

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

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil,
				adapter,
				nil,
				scheduler,
			)

			req := &pb.JobStatusRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
			}
			resp, err := JobRunServiceServer.JobStatus(ctx, req)
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

	t.Run("GetWindow", func(t *testing.T) {
		t.Run("should return the correct window date range", func(t *testing.T) {
			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				nil, nil, nil,
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
			resp, err := JobRunServiceServer.GetWindow(ctx, &req)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", resp.GetStart().AsTime().Format(time.RFC3339))
			assert.Equal(t, "2020-11-12T00:00:00Z", resp.GetEnd().AsTime().Format(time.RFC3339))
		})

		t.Run("should return error if any of the required fields in request is missing", func(t *testing.T) {
			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				nil, nil,
				nil, nil,
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
			_, err := JobRunServiceServer.GetWindow(ctx, &req)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = window size, offset and truncate_to must be provided", err.Error())
		})
	})
}
