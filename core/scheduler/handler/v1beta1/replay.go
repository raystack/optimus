package v1beta1

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type ReplayService interface {
	CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error)
	GetReplayList(ctx context.Context, projectName tenant.ProjectName) (replays []*scheduler.Replay, err error)
	GetReplayByID(ctx context.Context, replayID uuid.UUID) (replay *scheduler.ReplayWithRun, err error)
}

type ReplayHandler struct {
	l       log.Logger
	service ReplayService

	pb.UnimplementedReplayServiceServer
}

func (h ReplayHandler) Replay(ctx context.Context, req *pb.ReplayRequest) (*pb.ReplayResponse, error) {
	replayTenant, err := tenant.NewTenant(req.GetProjectName(), req.NamespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	if err = req.GetStartTime().CheckValid(); err != nil {
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid start_time"), "unable to start replay for "+req.GetJobName())
	}

	if req.GetEndTime() != nil {
		if err = req.GetEndTime().CheckValid(); err != nil {
			return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid end_time"), "unable to start replay for "+req.GetJobName())
		}
	}

	jobConfig := make(map[string]string)
	if req.JobConfig != "" {
		jobConfig, err = parseJobConfig(req.JobConfig)
		if err != nil {
			return nil, errors.GRPCErr(err, "unable to parse replay job config for "+req.JobName)
		}
	}

	replayConfig := scheduler.NewReplayConfig(req.GetStartTime().AsTime(), req.GetEndTime().AsTime(), req.Parallel, jobConfig, req.Description)
	replayID, err := h.service.CreateReplay(ctx, replayTenant, jobName, replayConfig)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	return &pb.ReplayResponse{Id: replayID.String()}, nil
}

func (h ReplayHandler) ListReplay(ctx context.Context, req *pb.ListReplayRequest) (*pb.ListReplayResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get replay list for "+req.GetProjectName())
	}

	replays, err := h.service.GetReplayList(ctx, projectName)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get replay list for "+req.GetProjectName())
	}

	replayProtos := make([]*pb.GetReplayResponse, len(replays))
	for i, replay := range replays {
		replayProtos[i] = replayToProto(replay)
	}

	return &pb.ListReplayResponse{Replays: replayProtos}, nil
}

func (h ReplayHandler) GetReplay(ctx context.Context, req *pb.GetReplayRequest) (*pb.GetReplayResponse, error) {
	id, err := uuid.Parse(req.GetReplayId())
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to get replay for replayID "+req.GetReplayId())
	}

	replay, err := h.service.GetReplayByID(ctx, id)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			return &pb.GetReplayResponse{}, nil
		}
		return nil, errors.GRPCErr(err, "unable to get replay for replayID "+req.GetReplayId())
	}

	runs := make([]*pb.ReplayRun, len(replay.Runs))
	for i, run := range replay.Runs {
		runs[i] = &pb.ReplayRun{
			ScheduledAt: timestamppb.New(run.ScheduledAt),
			Status:      run.State.String(),
		}
	}

	replayProto := replayToProto(replay.Replay)
	replayProto.ReplayRuns = runs

	return replayProto, nil
}

func replayToProto(replay *scheduler.Replay) *pb.GetReplayResponse {
	return &pb.GetReplayResponse{
		Id:      replay.ID().String(),
		JobName: replay.JobName().String(),
		Status:  replay.State().String(),
		ReplayConfig: &pb.ReplayConfig{
			StartTime:   timestamppb.New(replay.Config().StartTime),
			EndTime:     timestamppb.New(replay.Config().EndTime),
			Parallel:    replay.Config().Parallel,
			JobConfig:   replay.Config().JobConfig,
			Description: replay.Config().Description,
		},
	}
}

func parseJobConfig(jobConfig string) (map[string]string, error) {
	configs := map[string]string{}
	for _, config := range strings.Split(jobConfig, ",") {
		keyValue := strings.Split(config, "=")
		valueLen := 2
		if len(keyValue) != valueLen {
			return nil, fmt.Errorf("error on job config value, %s", config)
		}
		key := strings.TrimSpace(strings.ToUpper(keyValue[0]))
		value := keyValue[1]
		configs[key] = value
	}
	return configs, nil
}

func NewReplayHandler(l log.Logger, service ReplayService) *ReplayHandler {
	return &ReplayHandler{l: l, service: service}
}
