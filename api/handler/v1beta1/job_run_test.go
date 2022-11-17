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
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	AirflowDateFormat = "2006-01-02T15:04:05+00:00"
)

func TestJobRunServiceServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()
	monitoringService := new(mock.MonitoringService)

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
				monitoringService,
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
				monitoringService,
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
				monitoringService,
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
				monitoringService,
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
				monitoringService,
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
				monitoringService,
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
				monitoringService,
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
				monitoringService,
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

	t.Run("JobRunInput", func(t *testing.T) {
		projectName := "a-data-project"
		jobName := "a-data-job"

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
			ProjectSpec: projectSpec,
		}

		yamlPlugin1 := new(mock.YamlMod)

		window, err := models.NewWindow(1, "h", "24h", "24h")
		assert.Nil(t, err)
		jobSpec := models.JobSpec{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: jobName,
			Task: models.JobSpecTask{
				Window: window,
				Unit: &models.Plugin{
					YamlMod: yamlPlugin1,
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

		instanceName := "do-this"
		instanceType := models.InstanceTypeTask
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
		startTime, err := jobSpec.Task.Window.GetStartTime(scheduledAt)
		assert.Nil(t, err)
		endTime, err := jobSpec.Task.Window.GetEndTime(scheduledAt)
		assert.Nil(t, err)
		jobRunSpec := models.JobRunSpec{
			NamespaceID: namespaceSpec.ID,
			ProjectID:   projectSpec.ID.UUID(),
			ScheduledAt: scheduledAt,
			StartTime:   mockedTimeNow,
			Data: []models.JobRunSpecData{
				{
					Name:  models.ConfigKeyExecutionTime,
					Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  models.ConfigKeyDstart,
					Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  models.ConfigKeyDend,
					Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
			},
		}

		projectService := new(mock.ProjectService)
		projectService.On("Get", ctx, projectName).Return(projectSpec, nil)
		defer projectService.AssertExpectations(t)

		jobService := new(mock.JobService)
		jobService.On("GetByNameForProject", ctx, jobName, projectSpec).Return(jobSpec, namespaceSpec, nil)
		defer jobService.AssertExpectations(t)

		secretService := new(mock.SecretService)
		secretService.On("GetSecrets", ctx, namespaceSpec).Return(secrets, nil)
		defer secretService.AssertExpectations(t)

		monitoringService.On("GetJobRunByScheduledAt", ctx, namespaceSpec, jobSpec, scheduledAt).Return(jobRunSpec, nil)
		defer monitoringService.AssertExpectations(t)

		jobRunService := new(mock.JobRunService)

		jobRunInputCompiler := new(mock.JobInputCompiler)
		startTime, err = jobSpec.Task.Window.GetStartTime(scheduledAt)
		assert.Nil(t, err)
		endTime, err = jobSpec.Task.Window.GetEndTime(scheduledAt)
		assert.Nil(t, err)
		jobRunInputCompiler.On("Compile", ctx, namespaceSpec, models.ProjectSecrets(secrets), jobSpec, scheduledAt, jobRunSpec.Data, instanceType, instanceName).Return(
			&models.JobRunInput{
				ConfigMap: map[string]string{
					models.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					models.ConfigKeyDstart:        startTime.Format(models.InstanceScheduledAtTimeLayout),
					models.ConfigKeyDend:          endTime.Format(models.InstanceScheduledAtTimeLayout),
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
			monitoringService,
			nil,
		)

		jobRunInputRequest := pb.JobRunInputRequest{
			ProjectName:  projectName,
			JobName:      jobName,
			InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(string(models.InstanceTypeTask), "TYPE")]),
			ScheduledAt:  scheduledAtTimestamp,
			InstanceName: instanceName,
		}
		resp, err := JobRunServiceServer.JobRunInput(ctx, &jobRunInputRequest)
		assert.Nil(t, err)

		//TODO get stringified time
		startTime, err = jobSpec.Task.Window.GetStartTime(scheduledAt)
		assert.Nil(t, err)
		endTime, err = jobSpec.Task.Window.GetEndTime(scheduledAt)
		assert.Nil(t, err)
		expectedResponse := &pb.JobRunInputResponse{
			Envs: map[string]string{
				models.ConfigKeyExecutionTime: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
				models.ConfigKeyDstart:        startTime.Format(models.InstanceScheduledAtTimeLayout),
				models.ConfigKeyDend:          endTime.Format(models.InstanceScheduledAtTimeLayout),
			},
			Files: map[string]string{
				"query.sql": "select * from 1",
			},
		}
		assert.Equal(t, expectedResponse, resp)
	})
}
