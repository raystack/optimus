package v1beta1_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/handler/v1beta1"
	"github.com/odpf/optimus/core/tenant"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	AirflowDateFormat = "2006-01-02T15:04:05+00:00"
)

func TestJobRunHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	projectName := "a-data-proj"
	jobName := "a-job-name"

	t.Run("JobRunInput", func(t *testing.T) {
		t.Run("returns error when project name is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for "+
				"entity project: project name is empty: unable to get job run input for job1")
		})
		t.Run("returns error when job name is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: job name is empty: unable to get job run input for ")
		})
		t.Run("returns error when executor is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: executor name is invalid: unable to get job run input for job1")
		})
		t.Run("returns error when scheduled_at is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: invalid scheduled_at: unable to get job run input for job1")
		})
		t.Run("returns error when run config is invalid", func(t *testing.T) {
			service := new(mockJobRunService)
			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "1234",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"jobRun: invalid job run ID 1234: unable to get job run input for job1")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(mockJobRunService)
			service.On("JobRunInput", ctx, tenant.ProjectName("proj"), scheduler.JobName("job1"), mock.Anything).
				Return(&scheduler.ExecutorInput{}, fmt.Errorf("error in service"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			_, err := handler.JobRunInput(ctx, &inputRequest)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in service: unable to get job "+
				"run input for job1")
		})
		t.Run("returns job run input successfully", func(t *testing.T) {
			service := new(mockJobRunService)
			service.On("JobRunInput", ctx, tenant.ProjectName("proj"), scheduler.JobName("job1"), mock.Anything).
				Return(&scheduler.ExecutorInput{
					Configs: map[string]string{"a": "b"},
					Secrets: map[string]string{"name": "secret_value"},
					Files:   nil,
				}, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewJobRunHandler(logger, service, nil)

			inputRequest := pb.JobRunInputRequest{
				ProjectName:  "proj",
				JobName:      "job1",
				ScheduledAt:  timestamppb.Now(),
				InstanceName: "bq2bq",
				InstanceType: pb.InstanceSpec_TYPE_TASK,
				JobrunId:     "",
			}

			input, err := handler.JobRunInput(ctx, &inputRequest)
			assert.Nil(t, err)
			assert.Equal(t, "b", input.Envs["a"])
			assert.Equal(t, "secret_value", input.Secrets["name"])
		})
	})
	t.Run("JobRun", func(t *testing.T) {
		date, err := time.Parse(AirflowDateFormat, "2022-03-25T02:00:00+00:00")
		if err != nil {
			t.Errorf("unable to parse the time to test GetJobRuns %v", err)
		}
		t.Run("should return all job run via scheduler if valid inputs are given", func(t *testing.T) {
			job := scheduler.Job{
				Name: "transform-tables",
			}

			jobRuns := []*scheduler.JobRunStatus{{
				ScheduledAt: date,
				State:       scheduler.StateSuccess,
			}}
			query := &scheduler.JobRunsCriteria{
				Name:      job.Name.String(),
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRuns", ctx, tenant.ProjectName(projectName), job.Name, query).Return(jobRuns, nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil)

			req := &pb.JobRunRequest{
				ProjectName: projectName,
				JobName:     job.Name.String(),
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedStatus := range jobRuns {
				var found bool
				for _, respVal := range resp.JobRuns {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.State.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job Run status %v", expectedStatus))
				}
			}
		})
		t.Run("should return all job run via scheduler if valid inputs are given", func(t *testing.T) {
			job := scheduler.Job{
				Name: "transform-tables",
			}

			jobRuns := []*scheduler.JobRunStatus{{
				ScheduledAt: date,
				State:       scheduler.StateSuccess,
			}}
			query := &scheduler.JobRunsCriteria{
				Name:      job.Name.String(),
				StartDate: date,
				EndDate:   date.Add(time.Hour * 24),
				Filter:    []string{"success"},
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRuns", ctx, tenant.ProjectName(projectName), job.Name, query).Return(jobRuns, nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil)

			req := &pb.JobRunRequest{
				ProjectName: projectName,
				JobName:     job.Name.String(),
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, len(jobRuns), len(resp.JobRuns))
			for _, expectedStatus := range jobRuns {
				var found bool
				for _, respVal := range resp.JobRuns {
					if expectedStatus.ScheduledAt.Equal(respVal.ScheduledAt.AsTime()) &&
						expectedStatus.State.String() == respVal.State {
						found = true
						break
					}
				}
				if !found {
					assert.Fail(t, fmt.Sprintf("failed to find expected job Run status %v", expectedStatus))
				}
			}
		})
		t.Run("should return error if job run service raises error in GetJobRuns", func(t *testing.T) {
			job := scheduler.Job{
				Name: "transform-tables",
			}
			query := &scheduler.JobRunsCriteria{
				Name:        job.Name.String(),
				OnlyLastRun: true,
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("GetJobRuns", ctx, tenant.ProjectName(projectName), job.Name, query).Return(nil, fmt.Errorf("some random error"))
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil)

			req := &pb.JobRunRequest{
				ProjectName: projectName,
				JobName:     job.Name.String(),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = some random error: unable to get job run for transform-tables")
			assert.Nil(t, resp)
		})

		t.Run("should not return job runs if project name is not valid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "",
				JobName:     "transform-tables",
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get job run for transform-tables")
			assert.Nil(t, resp)
		})

		t.Run("should not return job runs if job name is not valid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "some-project",
				JobName:     "",
				StartDate:   timestamppb.New(date),
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: job name is empty: unable to get job run for ")
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if only start date is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "some-project",
				JobName:     "jobname",
				EndDate:     timestamppb.New(date.Add(time.Hour * 24)),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: empty start date is given: unable to get job run for jobname")
			assert.Nil(t, resp)
		})
		t.Run("should not return job runs if only end date is invalid", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)
			req := &pb.JobRunRequest{
				ProjectName: "some-project",
				JobName:     "jobname",
				StartDate:   timestamppb.New(date),
				Filter:      []string{"success"},
			}
			resp, err := jobRunHandler.JobRun(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: empty end date is given: unable to get job run for jobname")
			assert.Nil(t, resp)
		})
	})
	t.Run("UploadToScheduler", func(t *testing.T) {
		t.Run("should fail deployment if project name empty", func(t *testing.T) {
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)
			namespaceName := "namespace-name"
			req := &pb.UploadToSchedulerRequest{
				ProjectName:   "",
				NamespaceName: &namespaceName,
			}
			resp, err := jobRunHandler.UploadToScheduler(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get projectName")
			assert.Nil(t, resp)
		})
		t.Run("should fail deployment if UploadToScheduler service fails", func(t *testing.T) {
			namespaceName := "namespace-name"
			req := &pb.UploadToSchedulerRequest{
				ProjectName:   projectName,
				NamespaceName: &namespaceName,
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("UploadToScheduler", ctx, tenant.ProjectName(projectName), namespaceName).
				Return(fmt.Errorf("some error"))
			defer jobRunService.AssertExpectations(t)
			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil)

			resp, err := jobRunHandler.UploadToScheduler(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = some error: unsuccessful upload to scheduler for a-data-proj")
			assert.Nil(t, resp)
		})
		t.Run("should return success if deployment succeeds", func(t *testing.T) {
			namespaceName := "namespace-name"
			req := &pb.UploadToSchedulerRequest{
				ProjectName:   projectName,
				NamespaceName: &namespaceName,
			}
			jobRunService := new(mockJobRunService)
			jobRunService.On("UploadToScheduler", ctx, tenant.ProjectName(projectName), namespaceName).
				Return(nil)
			defer jobRunService.AssertExpectations(t)
			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, nil)

			resp, err := jobRunHandler.UploadToScheduler(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, true, resp.Status)
		})
		t.Run("should return error if projectName is not valid", func(t *testing.T) {
			namespaceName := "namespace-name"
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   "",
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  9,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity project: project name is empty: unable to get tenant")
			assert.Nil(t, resp)
		})

		t.Run("should return error if NamespaceName is not valid", func(t *testing.T) {
			namespaceName := ""
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  9,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity namespace: namespace name is empty: unable to get tenant")
			assert.Nil(t, resp)
		})
		t.Run("should return error if job name is not valid", func(t *testing.T) {
			namespaceName := "namespace-name"
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       "",
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  9,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity jobRun: job name is empty: unable to get job name")
			assert.Nil(t, resp)
		})

		t.Run("should return error on unregistered event type", func(t *testing.T) {
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			namespaceName := "some-namespace"
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  200,
					Value: eventValues,
				},
			}
			jobRunHandler := v1beta1.NewJobRunHandler(logger, nil, nil)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity event: unknown event 200: unable to parse event")
			assert.Nil(t, resp)
		})
		t.Run("should return error if Update Job State fails", func(t *testing.T) {
			namespaceName := "some-namespace"
			tnnt, _ := tenant.NewTenant(projectName, namespaceName)
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"event_time":   1600361600,
					"task_id":      "wait_sample_select",
					"status":       "success",
					"scheduled_at": "2022-01-02T15:04:05Z",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  pb.JobEvent_TYPE_TASK_SUCCESS,
					Value: eventValues,
				},
			}
			event, err := scheduler.EventFrom(
				req.GetEvent().Type.String(),
				req.GetEvent().Value.AsMap(),
				scheduler.JobName(jobName), tnnt,
			)
			assert.Nil(t, err)
			jobRunService := new(mockJobRunService)
			jobRunService.On("UpdateJobState", ctx, event).
				Return(fmt.Errorf("some error"))
			defer jobRunService.AssertExpectations(t)

			notifier := new(mockNotifier)
			notifier.On("Push", ctx, event).
				Return(nil)
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, notifier)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "errors in RegisterJobEvent:\n internal error for entity jobRun: scheduler could not update job run state, err:some error")
			assert.Equal(t, &pb.RegisterJobEventResponse{}, resp)
		})
		t.Run("should return error if notify Push fails", func(t *testing.T) {
			namespaceName := "some-namespace"
			tnnt, _ := tenant.NewTenant(projectName, namespaceName)
			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url":          "https://example.io",
					"event_time":   1600361600,
					"status":       "success",
					"scheduled_at": "2022-01-02T15:04:05Z",
					"task_id":      "wait_sample_select",
				},
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectName,
				JobName:       jobName,
				NamespaceName: namespaceName,
				Event: &pb.JobEvent{
					Type:  pb.JobEvent_TYPE_TASK_SUCCESS,
					Value: eventValues,
				},
			}
			event, err := scheduler.EventFrom(
				req.GetEvent().Type.String(),
				req.GetEvent().Value.AsMap(),
				scheduler.JobName(jobName), tnnt,
			)
			assert.Nil(t, err)
			jobRunService := new(mockJobRunService)
			jobRunService.On("UpdateJobState", ctx, event).
				Return(nil)
			defer jobRunService.AssertExpectations(t)

			notifier := new(mockNotifier)
			notifier.On("Push", ctx, event).
				Return(fmt.Errorf("some error"))
			defer jobRunService.AssertExpectations(t)

			jobRunHandler := v1beta1.NewJobRunHandler(logger, jobRunService, notifier)

			resp, err := jobRunHandler.RegisterJobEvent(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "errors in RegisterJobEvent:\n some error")
			assert.Equal(t, &pb.RegisterJobEventResponse{}, resp)
		})
	})
}

type mockJobRunService struct {
	mock.Mock
}

func (m *mockJobRunService) JobRunInput(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, config scheduler.RunConfig) (*scheduler.ExecutorInput, error) {
	args := m.Called(ctx, projectName, jobName, config)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*scheduler.ExecutorInput), args.Error(1)
}

func (m *mockJobRunService) UpdateJobState(ctx context.Context, event scheduler.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *mockJobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName, namespaceName string) error {
	args := m.Called(ctx, projectName, namespaceName)
	return args.Error(0)
}

func (m *mockJobRunService) GetJobRuns(ctx context.Context, projectName tenant.ProjectName, jobName scheduler.JobName, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	args := m.Called(ctx, projectName, jobName, criteria)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*scheduler.JobRunStatus), args.Error(1)
}

type mockNotifier struct {
	mock.Mock
}

func (m *mockNotifier) Push(ctx context.Context, event scheduler.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}
