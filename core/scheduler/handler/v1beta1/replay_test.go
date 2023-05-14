package v1beta1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/handler/v1beta1"
	"github.com/odpf/optimus/core/tenant"
	errs "github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func TestReplayHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	projectName := "a-data-proj"
	namespaceName := "a-namespace"
	jobTenant, _ := tenant.NewTenant(projectName, namespaceName)
	jobName, _ := scheduler.JobNameFrom("a-job-name")
	startTime := timestamppb.New(time.Date(2023, 0o1, 0o1, 13, 0, 0, 0, time.UTC))
	endTime := timestamppb.New(time.Date(2023, 0o1, 0o2, 13, 0, 0, 0, time.UTC))
	jobConfigStr := "EXECUTION_PROJECT=example_project,ANOTHER_CONFIG=example_value"
	jobConfig := map[string]string{"EXECUTION_PROJECT": "example_project", "ANOTHER_CONFIG": "example_value"}
	description := "sample backfill"
	replayID := uuid.New()

	t.Run("CreateReplay", func(t *testing.T) {
		t.Run("returns replay ID when able to create replay successfully", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				StartTime:     startTime,
				EndTime:       endTime,
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, jobConfig, description)

			service.On("CreateReplay", ctx, jobTenant, jobName, replayConfig).Return(replayID, nil)

			result, err := replayHandler.Replay(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, replayID.String(), result.Id)
		})
		t.Run("returns replay ID when able to create replay successfully without overriding job config", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				StartTime:     startTime,
				EndTime:       endTime,
				Parallel:      false,
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, map[string]string{}, description)

			service.On("CreateReplay", ctx, jobTenant, jobName, replayConfig).Return(replayID, nil)

			result, err := replayHandler.Replay(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, replayID.String(), result.Id)
		})
		t.Run("returns error when unable to create tenant", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				StartTime:     startTime,
				EndTime:       endTime,
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}

			result, err := replayHandler.Replay(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns error when job name is invalid", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				NamespaceName: namespaceName,
				StartTime:     startTime,
				EndTime:       endTime,
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}

			result, err := replayHandler.Replay(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns error when start time is invalid", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				EndTime:       endTime,
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}

			result, err := replayHandler.Replay(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns no error when end time is empty", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				StartTime:     startTime,
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, jobConfig, description)

			service.On("CreateReplay", ctx, jobTenant, jobName, replayConfig).Return(replayID, nil)

			result, err := replayHandler.Replay(ctx, req)
			assert.NoError(t, err)
			assert.Equal(t, replayID.String(), result.Id)
		})
		t.Run("returns error when end time is present but invalid", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				StartTime:     startTime,
				EndTime:       timestamppb.New(time.Date(-1, 13, 0o2, 13, 0, 0, 0, time.UTC)),
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, jobConfig, description)

			service.On("CreateReplay", ctx, jobTenant, jobName, replayConfig).Return(replayID, nil)

			result, err := replayHandler.Replay(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("returns error when unable to create replay", func(t *testing.T) {
			service := new(mockReplayService)
			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ReplayRequest{
				ProjectName:   projectName,
				JobName:       jobName.String(),
				NamespaceName: namespaceName,
				StartTime:     startTime,
				EndTime:       endTime,
				Parallel:      false,
				JobConfig:     jobConfigStr,
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, jobConfig, description)

			service.On("CreateReplay", ctx, jobTenant, jobName, replayConfig).Return(uuid.Nil, errors.New("internal error"))

			result, err := replayHandler.Replay(ctx, req)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
	})

	t.Run("GetReplayList", func(t *testing.T) {
		t.Run("return error when project name is not provided", func(t *testing.T) {
			service := new(mockReplayService)
			defer service.AssertExpectations(t)

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ListReplayRequest{
				ProjectName: "",
			}

			result, err := replayHandler.ListReplay(ctx, req)
			assert.ErrorContains(t, err, "project name is empty")
			assert.Nil(t, result)
		})
		t.Run("return error when get replay list failed", func(t *testing.T) {
			service := new(mockReplayService)
			service.On("GetReplayList", ctx, tenant.ProjectName("project-test")).Return(nil, errors.New("some error"))
			defer service.AssertExpectations(t)

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ListReplayRequest{
				ProjectName: "project-test",
			}

			result, err := replayHandler.ListReplay(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, result)
		})
		t.Run("return empty list when no replay found in project", func(t *testing.T) {
			service := new(mockReplayService)
			service.On("GetReplayList", ctx, tenant.ProjectName("project-test")).Return([]*scheduler.Replay{}, nil)
			defer service.AssertExpectations(t)

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ListReplayRequest{
				ProjectName: "project-test",
			}

			result, err := replayHandler.ListReplay(ctx, req)
			assert.NoError(t, err)
			assert.Empty(t, result.Replays)
		})
		t.Run("return replay list when success", func(t *testing.T) {
			tnnt, _ := tenant.NewTenant("project-test", "ns-1")
			startTimeStr := "2023-01-02T15:00:00Z"
			startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
			endTime := startTime.Add(48 * time.Hour)
			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, map[string]string{}, description)

			replay1 := scheduler.NewReplayRequest("sample-job-A", tnnt, replayConfig, scheduler.ReplayStateInProgress)
			replay2 := scheduler.NewReplayRequest("sample-job-B", tnnt, replayConfig, scheduler.ReplayStateCreated)
			replay3 := scheduler.NewReplayRequest("sample-job-C", tnnt, replayConfig, scheduler.ReplayStateFailed)
			service := new(mockReplayService)
			service.On("GetReplayList", ctx, tenant.ProjectName("project-test")).Return([]*scheduler.Replay{replay1, replay2, replay3}, nil)
			defer service.AssertExpectations(t)

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.ListReplayRequest{
				ProjectName: "project-test",
			}

			result, err := replayHandler.ListReplay(ctx, req)
			assert.NoError(t, err)
			assert.Len(t, result.Replays, 3)
		})
	})

	t.Run("GetReplay", func(t *testing.T) {
		t.Run("returns error when uuid is not valid", func(t *testing.T) {
			replayHandler := v1beta1.NewReplayHandler(logger, nil)

			req := &pb.GetReplayRequest{
				ProjectName: projectName,
				ReplayId:    "invalid-id",
			}
			result, err := replayHandler.GetReplay(ctx, req)
			assert.ErrorContains(t, err, "invalid UUID")
			assert.Nil(t, result)
		})
		t.Run("returns error when service get replay by id is failed", func(t *testing.T) {
			service := new(mockReplayService)
			defer service.AssertExpectations(t)

			replayID := uuid.New()
			service.On("GetReplayByID", ctx, replayID).Return(nil, errors.New("internal error"))

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.GetReplayRequest{
				ProjectName: projectName,
				ReplayId:    replayID.String(),
			}
			result, err := replayHandler.GetReplay(ctx, req)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
		t.Run("returns empty if replay not exist", func(t *testing.T) {
			service := new(mockReplayService)
			defer service.AssertExpectations(t)

			replayID := uuid.New()
			service.On("GetReplayByID", ctx, replayID).Return(nil, errs.NotFound("entity", "not found"))

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.GetReplayRequest{
				ProjectName: projectName,
				ReplayId:    replayID.String(),
			}
			result, err := replayHandler.GetReplay(ctx, req)
			assert.NoError(t, err)
			assert.Empty(t, result)
		})
		t.Run("returns success if replay is exist", func(t *testing.T) {
			service := new(mockReplayService)
			defer service.AssertExpectations(t)

			replayID := uuid.New()
			tnnt, _ := tenant.NewTenant("project-test", "ns-1")
			startTimeStr := "2023-01-02T15:00:00Z"
			startTime, _ := time.Parse(scheduler.ISODateFormat, startTimeStr)
			endTime := startTime.Add(48 * time.Hour)
			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, map[string]string{}, description)
			replay := scheduler.NewReplay(replayID, "sample-job-A", tnnt, replayConfig, scheduler.ReplayStateInProgress, startTime)
			service.On("GetReplayByID", ctx, replayID).Return(&scheduler.ReplayWithRun{
				Replay: replay,
				Runs: []*scheduler.JobRunStatus{
					{
						ScheduledAt: startTime,
						State:       scheduler.StatePending,
					},
				},
			}, nil)

			replayHandler := v1beta1.NewReplayHandler(logger, service)

			req := &pb.GetReplayRequest{
				ProjectName: projectName,
				ReplayId:    replayID.String(),
			}
			result, err := replayHandler.GetReplay(ctx, req)
			assert.NoError(t, err)
			assert.NotEmpty(t, result)
		})
	})
}

// mockReplayService is an autogenerated mock type for the ReplayService type
type mockReplayService struct {
	mock.Mock
}

// GetReplayByID provides a mock function with given fields: ctx, replayID
func (_m *mockReplayService) GetReplayByID(ctx context.Context, replayID uuid.UUID) (*scheduler.ReplayWithRun, error) {
	ret := _m.Called(ctx, replayID)

	var r0 *scheduler.ReplayWithRun
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) (*scheduler.ReplayWithRun, error)); ok {
		return rf(ctx, replayID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, uuid.UUID) *scheduler.ReplayWithRun); ok {
		r0 = rf(ctx, replayID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*scheduler.ReplayWithRun)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, uuid.UUID) error); ok {
		r1 = rf(ctx, replayID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateReplay provides a mock function with given fields: ctx, _a1, jobName, config
func (_m *mockReplayService) CreateReplay(ctx context.Context, _a1 tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (uuid.UUID, error) {
	ret := _m.Called(ctx, _a1, jobName, config)

	var r0 uuid.UUID
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant, scheduler.JobName, *scheduler.ReplayConfig) uuid.UUID); ok {
		r0 = rf(ctx, _a1, jobName, config)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(uuid.UUID)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant, scheduler.JobName, *scheduler.ReplayConfig) error); ok {
		r1 = rf(ctx, _a1, jobName, config)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetReplayList provides a mock function with given fields: ctx, projectName
func (_m *mockReplayService) GetReplayList(ctx context.Context, projectName tenant.ProjectName) ([]*scheduler.Replay, error) {
	ret := _m.Called(ctx, projectName)

	var r0 []*scheduler.Replay
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) ([]*scheduler.Replay, error)); ok {
		return rf(ctx, projectName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) []*scheduler.Replay); ok {
		r0 = rf(ctx, projectName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*scheduler.Replay)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName) error); ok {
		r1 = rf(ctx, projectName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
