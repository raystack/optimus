package v1beta1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/handler/v1beta1"
	"github.com/odpf/optimus/core/tenant"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

//const (
//	AirflowDateFormat = "2006-01-02T15:04:05+00:00"
//)

func TestJobRunHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()

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
				Return(scheduler.ExecutorInput{}, errors.New("error in service"))
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
				Return(scheduler.ExecutorInput{
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
