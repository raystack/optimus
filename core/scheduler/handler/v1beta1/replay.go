package v1beta1

import (
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
	GetRunsStatus(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (runs []*scheduler.JobRunStatus, err error)
}

type replayRequest interface {
	GetProjectName() string
	GetNamespaceName() string
	GetJobName() string
	GetStartTime() *timestamppb.Timestamp
	GetEndTime() *timestamppb.Timestamp
	GetJobConfig() string
	GetParallel() bool
	GetDescription() string
}

type ReplayHandler struct {
	l       log.Logger
	service ReplayService

	pb.UnimplementedReplayServiceServer
}

func (h ReplayHandler) ReplayDryRun(ctx context.Context, req *pb.ReplayDryRunRequest) (*pb.ReplayDryRunResponse, error) {
	replayReq, err := newReplayRequest(h.l, req)
	if err != nil {
		return nil, err
	}

	runs, err := h.service.GetRunsStatus(ctx, replayReq.Tenant(), replayReq.JobName(), replayReq.Config())
	if err != nil {
		h.l.Error("error fetching runs status for replay dry run: %s", err)
		return nil, errors.GRPCErr(err, "unable to fetch runs status for "+req.JobName)
	}

	return &pb.ReplayDryRunResponse{
		ReplayRuns: replayRunsToProto(runs),
	}, nil
}

func (h ReplayHandler) Replay(ctx context.Context, req *pb.ReplayRequest) (*pb.ReplayResponse, error) {
	replayReq, err := newReplayRequest(h.l, req)
	if err != nil {
		return nil, err
	}

	replayID, err := h.service.CreateReplay(ctx, replayReq.Tenant(), replayReq.JobName(), replayReq.Config())
	if err != nil {
		h.l.Error("error creating replay for job [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	return &pb.ReplayResponse{Id: replayID.String()}, nil
}

func (h ReplayHandler) ListReplay(ctx context.Context, req *pb.ListReplayRequest) (*pb.ListReplayResponse, error) {
	projectName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		h.l.Error("error adapting project name [%s]: %s", req.GetProjectName(), err)
		return nil, errors.GRPCErr(err, "unable to get replay list for "+req.GetProjectName())
	}

	replays, err := h.service.GetReplayList(ctx, projectName)
	if err != nil {
		h.l.Error("error getting replay list for project [%s]: %s", projectName, err)
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
		h.l.Error("error parsing replay id [%s]: %s", req.GetReplayId(), err)
		err = errors.InvalidArgument(scheduler.EntityReplay, err.Error())
		return nil, errors.GRPCErr(err, "unable to get replay for replayID "+req.GetReplayId())
	}

	replay, err := h.service.GetReplayByID(ctx, id)
	if err != nil {
		if errors.IsErrorType(err, errors.ErrNotFound) {
			h.l.Warn("replay with id [%s] is not found", id.String())
			return &pb.GetReplayResponse{}, nil
		}
		h.l.Error("error getting replay with id [%s]: %s", id.String(), err)
		return nil, errors.GRPCErr(err, "unable to get replay for replayID "+req.GetReplayId())
	}

	replayProto := replayToProto(replay.Replay)
	replayProto.ReplayRuns = replayRunsToProto(replay.Runs)

	return replayProto, nil
}

func replayRunsToProto(runs []*scheduler.JobRunStatus) []*pb.ReplayRun {
	runsProto := make([]*pb.ReplayRun, len(runs))
	for i, run := range runs {
		runsProto[i] = &pb.ReplayRun{
			ScheduledAt: timestamppb.New(run.ScheduledAt),
			Status:      run.State.String(),
		}
	}
	return runsProto
}

func newReplayRequest(l log.Logger, req replayRequest) (*scheduler.Replay, error) {
	replayTenant, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	jobName, err := scheduler.JobNameFrom(req.GetJobName())
	if err != nil {
		l.Error("error adapting job name [%s]: %s", req.GetJobName(), err)
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	if err = req.GetStartTime().CheckValid(); err != nil {
		l.Error("error validating start time: %s", err)
		return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid start_time"), "unable to start replay for "+req.GetJobName())
	}

	if req.GetEndTime() != nil {
		if err = req.GetEndTime().CheckValid(); err != nil {
			l.Error("error validating end time: %s", err)
			return nil, errors.GRPCErr(errors.InvalidArgument(scheduler.EntityJobRun, "invalid end_time"), "unable to start end for "+req.GetJobName())
		}
	}

	jobConfig := make(map[string]string)
	if req.GetJobConfig() != "" {
		jobConfig, err = parseJobConfig(req.GetJobConfig())
		if err != nil {
			return nil, err
		}
	}

	replayConfig := scheduler.NewReplayConfig(req.GetStartTime().AsTime(), req.GetEndTime().AsTime(), req.GetParallel(), jobConfig, req.GetDescription())
	if err != nil {
		l.Error("error parsing job config: %s", err)
		return nil, errors.GRPCErr(err, "unable to parse replay job config for "+req.GetJobName())
	}

	return scheduler.NewReplayRequest(jobName, replayTenant, replayConfig, scheduler.ReplayStateCreated), nil
}

func replayToProto(replay *scheduler.Replay) *pb.GetReplayResponse {
	return &pb.GetReplayResponse{
		Id:      replay.ID().String(),
		JobName: replay.JobName().String(),
		Status:  replay.UserState().String(),
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
			return nil, errors.InvalidArgument(scheduler.EntityReplay, "error on job config value, "+config)
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
