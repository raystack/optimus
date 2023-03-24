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
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, description)

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
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, description)

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
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, description)

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
				Description:   description,
			}
			replayConfig := scheduler.NewReplayConfig(req.StartTime.AsTime(), req.EndTime.AsTime(), false, description)

			service.On("CreateReplay", ctx, jobTenant, jobName, replayConfig).Return(uuid.Nil, errors.New("internal error"))

			result, err := replayHandler.Replay(ctx, req)
			assert.ErrorContains(t, err, "internal error")
			assert.Nil(t, result)
		})
	})
}

// mockReplayService is an autogenerated mock type for the ReplayService type
type mockReplayService struct {
	mock.Mock
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
