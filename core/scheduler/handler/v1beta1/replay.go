package v1beta1

import (
	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type ReplayService interface {
	CreateReplay(ctx context.Context, tenant tenant.Tenant, jobName scheduler.JobName, config *scheduler.ReplayConfig) (replayID uuid.UUID, err error)
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

	replayConfig := scheduler.NewReplayConfig(req.GetStartTime().AsTime(), req.GetEndTime().AsTime(), req.Parallel, req.Description)
	replayID, err := h.service.CreateReplay(ctx, replayTenant, jobName, replayConfig)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to start replay for "+req.GetJobName())
	}

	return &pb.ReplayResponse{Id: replayID.String()}, nil
}

func NewReplayHandler(l log.Logger, service ReplayService) *ReplayHandler {
	return &ReplayHandler{l: l, service: service}
}
