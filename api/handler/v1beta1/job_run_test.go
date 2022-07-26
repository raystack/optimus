package v1beta1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/utils"
)

const (
	AirflowDateFormat = "2006-01-02T15:04:05+00:00"
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
			ID:   models.ProjectID(uuid.New()),
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
					Name:  models.ConfigKeyExecutionTime,
					Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  models.ConfigKeyDstart,
					Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  models.ConfigKeyDend,
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
		secrets := []models.ProjectSecretItem{
			{
				ID:    uuid.New(),
				Name:  "table_name",
				Value: "secret_table",
				Type:  models.SecretTypeUserDefined,
			},
			{
				ID:    uuid.New(),
				Name:  "bucket",
				Value: "gs://some_secret_bucket",
				Type:  models.SecretTypeUserDefined,
			},
		}
		t.Run("should register a new job instance with run for scheduled triggers", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobName, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			secretService := new(mock.SecretService)
			secretService.On("GetSecrets", ctx, namespaceSpec).Return(secrets, nil)
			defer secretService.AssertExpectations(t)

			jobRunService := new(mock.JobRunService)
			jobRunService.On("GetScheduledRun", ctx, namespaceSpec, jobSpec, scheduledAt).Return(jobRun, nil)
			jobRunService.On("Register", ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name).Return(instanceSpec, nil)
			defer jobRunService.AssertExpectations(t)

			jobRunInputCompiler := new(mock.JobInputCompiler)
			jobRunInputCompiler.On("Compile", ctx, namespaceSpec, models.ProjectSecrets(secrets), jobRun, instanceSpec).Return(
				&models.JobRunInput{
					ConfigMap: map[string]string{
						models.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					FileMap: map[string]string{
						"query.sql": "select * from 1",
					},
				}, nil)
			defer jobRunInputCompiler.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService,
				projectService, nil, secretService,
				nil,
				jobRunService,
				jobRunInputCompiler,
				nil,
			)

			registerInstanceRequest := pb.RegisterInstanceRequest{
				ProjectName:  projectName,
				JobName:      jobName,
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
				ScheduledAt:  scheduledAtTimestamp,
				InstanceName: instanceSpec.Name,
			}
			resp, err := JobRunServiceServer.RegisterInstance(ctx, &registerInstanceRequest)
			assert.Nil(t, err)

			projectSpecProto := v1.ToProjectProto(projectSpec)
			jobSpecProto := v1.ToJobSpecificationProto(jobSpec)
			instanceSpecProto := v1.ToInstanceProto(instanceSpec)
			expectedResponse := &pb.RegisterInstanceResponse{
				Job: jobSpecProto, Instance: instanceSpecProto,
				Project: projectSpecProto,
				Context: &pb.InstanceContext{
					Envs: map[string]string{
						models.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					Files: map[string]string{
						"query.sql": "select * from 1",
					},
				},
				Namespace: v1.ToNamespaceProto(namespaceSpec),
			}

			assert.Equal(t, expectedResponse, resp)
		})
		t.Run("should find the existing job run if manually triggered", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			instanceService := new(mock.JobRunService)
			instanceService.On("GetByID", ctx, jobRun.ID).Return(jobRun, namespaceSpec, nil)
			instanceService.On("Register", ctx, namespaceSpec, jobRun, instanceSpec.Type, instanceSpec.Name).Return(instanceSpec, nil)
			defer instanceService.AssertExpectations(t)

			secretService := new(mock.SecretService)
			secretService.On("GetSecrets", ctx, namespaceSpec).Return(secrets, nil)
			defer secretService.AssertExpectations(t)

			jobRunInputCompiler := new(mock.JobInputCompiler)
			jobRunInputCompiler.On("Compile", ctx, namespaceSpec, models.ProjectSecrets(secrets), jobRun, instanceSpec).Return(
				&models.JobRunInput{
					ConfigMap: map[string]string{
						models.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					FileMap: map[string]string{
						"query.sql": "select * from 1",
					},
				}, nil)
			defer jobRunInputCompiler.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				nil,
				projectService,
				nil,
				secretService,
				nil,
				instanceService,
				jobRunInputCompiler,
				nil,
			)

			versionRequest := pb.RegisterInstanceRequest{
				ProjectName:  projectName,
				JobrunId:     jobRun.ID.String(),
				InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
				InstanceName: instanceSpec.Name,
			}
			resp, err := JobRunServiceServer.RegisterInstance(ctx, &versionRequest)
			assert.Nil(t, err)

			projectSpecProto := v1.ToProjectProto(projectSpec)
			jobSpecProto := v1.ToJobSpecificationProto(jobSpec)
			instanceSpecProto := v1.ToInstanceProto(instanceSpec)
			expectedResponse := &pb.RegisterInstanceResponse{
				Job: jobSpecProto, Instance: instanceSpecProto,
				Project: projectSpecProto,
				Context: &pb.InstanceContext{
					Envs: map[string]string{
						models.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						models.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
					},
					Files: map[string]string{
						"query.sql": "select * from 1",
					},
				},
				Namespace: v1.ToNamespaceProto(namespaceSpec),
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
				ID:   models.ProjectID(uuid.New()),
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
				nsService,
				nil,
				allTasksRepo,
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
			jobTaskResp, err := JobRunServiceServer.GetJobTask(ctx, jobTaskRequest)
			assert.Nil(t, err)
			assert.Equal(t, taskSpecExpected, jobTaskResp.Task)
		})
		t.Run("task without dependency mod should skip destination and dependency fields", func(t *testing.T) {
			projectName := "a-data-project"
			jobName1 := "a-data-job"
			taskName := "a-data-task"

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			jobService.On("GetTaskDependencies", ctx, namespaceSpec, jobSpecs[0]).Return(models.JobSpecTaskDestination{},
				models.JobSpecTaskDependencies([]string{}), service.ErrDependencyModNotFound)
			defer jobService.AssertExpectations(t)

			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService,
				nil,
				namespaceService,
				nil,
				allTasksRepo,
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
			jobTaskResp, err := JobRunServiceServer.GetJobTask(ctx, jobTaskRequest)
			assert.Nil(t, err)
			assert.Equal(t, taskSpecExpected, jobTaskResp.Task)
		})
	})

	t.Run("JobStatus", func(t *testing.T) {
		t.Run("should return all job status via scheduler if valid inputs", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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
				jobService, projectService, nil, nil,
				nil,
				nil,
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
				nil, nil, nil, nil,
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
			resp, err := JobRunServiceServer.GetWindow(ctx, &req)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", resp.GetStart().AsTime().Format(time.RFC3339))
			assert.Equal(t, "2020-11-12T00:00:00Z", resp.GetEnd().AsTime().Format(time.RFC3339))
		})

		t.Run("should return error if any of the required fields in request is missing", func(t *testing.T) {
			JobRunServiceServer := v1.NewJobRunServiceServer(
				log,
				nil, nil,
				nil, nil, nil,
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
			_, err := JobRunServiceServer.GetWindow(ctx, &req)
			assert.Equal(t, "rpc error: code = InvalidArgument desc = window size, offset and truncate_to must be provided", err.Error())
		})
	})
	t.Run("JobRun", func(t *testing.T) {
		date, err := time.Parse(AirflowDateFormat, "2022-03-25T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		t.Run("should return all job run via scheduler if valid inputs are given", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			jobRuns := []models.JobRun{
				{
					ScheduledAt: date,
					Status:      "success",
				},
			}
			query := &models.JobQuery{
				Name:      jobSpec.Name,
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			instsvc := new(mock.JobRunService)
			instsvc.On("GetJobRunList", ctx, projectSpec, jobSpec, query).Return(jobRuns, nil)
			defer instsvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				instsvc,
				nil,
				nil,
			)

			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedStatus := range jobRuns {
				var found bool
				for _, respVal := range resp.JobRuns {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.Status.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job Run status %v", expectedStatus))
				}
			}
		})
		t.Run("should not return job runs if project is not found at DB", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "a-data-project",
			}

			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectSpec.Name).Return(models.ProjectSpec{}, errors.New("no project found"))
			defer projectService.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				nil, projectService, nil, nil, nil,
				nil,
				nil,
				nil,
			)
			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     "transform-tables",
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp, nil)
		})
		t.Run("should not return job runs if job spec is not found at DB", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "a-data-project",
			}

			jobSpec := models.JobSpec{
				Name: "transform-tables",
			}

			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectSpec.Name).Return(projectSpec, nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(models.JobSpec{}, models.NamespaceSpec{}, errors.New("no job spec found"))
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				nil,
				nil,
				nil,
			)

			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     "transform-tables",
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp, nil)
		})
		t.Run("should not return job runs if start date is empty", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				nil,
				nil,
				nil,
			)

			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
				StartDate:   timestamppb.New(time.Unix(0, 0)),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if end date is empty", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				nil,
				nil,
				nil,
			)

			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(time.Unix(0, 0)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if scheduler is not reachable", func(t *testing.T) {
			date, err := time.Parse(AirflowDateFormat, "2022-03-25T02:00:00+00:00")
			if err != nil {
				t.Errorf("unable to parse the time to test GetJobRuns %v", err)
			}

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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
			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			query := &models.JobQuery{
				Name:      jobSpec.Name,
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			instsvc := new(mock.JobRunService)
			instsvc.On("GetJobRunList", ctx, projectSpec, jobSpec, query).Return([]models.JobRun{}, errors.New("failed due to wrong URL"))
			defer instsvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				instsvc,
				nil,
				nil,
			)

			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if scheduler return empty response", func(t *testing.T) {
			date, err := time.Parse(AirflowDateFormat, "2022-03-25T02:00:00+00:00")
			if err != nil {
				t.Errorf("unable to parse the time to test GetJobRuns %v", err)
			}

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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
			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			query := &models.JobQuery{
				Name:      jobSpec.Name,
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			instsvc := new(mock.JobRunService)
			instsvc.On("GetJobRunList", ctx, projectSpec, jobSpec, query).Return([]models.JobRun{}, nil)
			defer instsvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				instsvc,
				nil,
				nil,
			)

			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.Nil(t, resp)
		})
		t.Run("should return job runs if date range is empty", func(t *testing.T) {
			date, err := time.Parse(AirflowDateFormat, "2022-03-25T02:00:00+00:00")
			if err != nil {
				t.Errorf("unable to parse the time to test GetJobRuns %v", err)
			}
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
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

			jobService := new(mock.JobService)
			jobService.On("GetByNameForProject", ctx, jobSpec.Name, projectSpec).Return(jobSpec, namespaceSpec, nil)
			defer jobService.AssertExpectations(t)

			jobRuns := []models.JobRun{
				{
					ScheduledAt: date,
					Status:      "success",
				},
			}
			query := &models.JobQuery{
				Name:        jobSpec.Name,
				OnlyLastRun: true,
			}
			instsvc := new(mock.JobRunService)
			instsvc.On("GetJobRunList", ctx, projectSpec, jobSpec, query).Return(jobRuns, nil)
			defer instsvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewJobRunServiceServer(
				log,
				jobService, projectService, nil, nil, nil,
				instsvc,
				nil,
				nil,
			)
			req := &pb.JobRunRequest{
				ProjectName: projectSpec.Name,
				JobName:     jobSpec.Name,
				StartDate:   timestamppb.New(time.Unix(0, 0)),
				EndDate:     timestamppb.New(time.Unix(0, 0)),
				Filter:      []string{"success"},
			}
			resp, err := runtimeServiceServer.JobRun(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedStatus := range jobRuns {
				var found bool
				for _, respVal := range resp.JobRuns {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.Status.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job Run status %v", expectedStatus))
				}
			}
		})
	})
}
