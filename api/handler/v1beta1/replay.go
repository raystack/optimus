package v1beta1

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/uuid"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (sv *RuntimeServiceServer) ReplayDryRun(ctx context.Context, req *pb.ReplayDryRunRequest) (*pb.ReplayDryRunResponse, error) {
	replayRequest, err := sv.parseReplayRequest(ctx, req.ProjectName, req.NamespaceName, req.JobName, req.StartDate,
		req.EndDate, false, req.AllowedDownstreamNamespaces)
	if err != nil {
		return nil, err
	}

	replayPlan, err := sv.jobSvc.ReplayDryRun(ctx, replayRequest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while processing replay dry run: %v", err)
	}

	node, err := sv.adapter.ToReplayExecutionTreeNode(replayPlan.ExecutionTree)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while preparing replay dry run response: %v", err)
	}
	return &pb.ReplayDryRunResponse{
		Success:       true,
		ExecutionTree: node,
		IgnoredJobs:   replayPlan.IgnoredJobs,
	}, nil
}

func (sv *RuntimeServiceServer) Replay(ctx context.Context, req *pb.ReplayRequest) (*pb.ReplayResponse, error) {
	replayWorkerRequest, err := sv.parseReplayRequest(ctx, req.ProjectName, req.NamespaceName, req.JobName, req.StartDate,
		req.EndDate, req.Force, req.AllowedDownstreamNamespaces)
	if err != nil {
		return nil, err
	}

	replayResult, err := sv.jobSvc.Replay(ctx, replayWorkerRequest)
	if err != nil {
		if errors.Is(err, job.ErrRequestQueueFull) {
			return nil, status.Errorf(codes.Unavailable, "error while processing replay: %v", err)
		} else if errors.Is(err, job.ErrConflictedJobRun) {
			return nil, status.Errorf(codes.FailedPrecondition, "error while validating replay: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "error while processing replay: %v", err)
	}

	return &pb.ReplayResponse{
		Id:          replayResult.ID.String(),
		IgnoredJobs: replayResult.IgnoredJobs,
	}, nil
}

func (sv *RuntimeServiceServer) GetReplayStatus(ctx context.Context, req *pb.GetReplayStatusRequest) (*pb.GetReplayStatusResponse, error) {
	replayRequest, err := sv.parseReplayStatusRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	replayState, err := sv.jobSvc.GetReplayStatus(ctx, replayRequest)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting replay: %v", err)
	}

	node, err := sv.adapter.ToReplayStatusTreeNode(replayState.Node)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting replay status tree: %v", err)
	}

	return &pb.GetReplayStatusResponse{
		State:    replayState.Status,
		Response: node,
	}, nil
}

func (sv *RuntimeServiceServer) parseReplayStatusRequest(ctx context.Context, req *pb.GetReplayStatusRequest) (models.ReplayRequest, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return models.ReplayRequest{}, mapToGRPCErr(sv.l, err, fmt.Sprintf("not able to find project %s", req.GetProjectName()))
	}

	uuid, err := uuid.Parse(req.Id)
	if err != nil {
		return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "error while parsing replay ID: %v", err)
	}

	replayRequest := models.ReplayRequest{
		ID:      uuid,
		Project: projSpec,
	}
	return replayRequest, nil
}

func (sv *RuntimeServiceServer) ListReplays(ctx context.Context, req *pb.ListReplaysRequest) (*pb.ListReplaysResponse, error) {
	projSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, fmt.Sprintf("not able to find project %s", req.GetProjectName()))
	}

	replays, err := sv.jobSvc.GetReplayList(ctx, projSpec.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting replay list: %v", err)
	}

	var replaySpecs []*pb.ReplaySpec
	for _, replaySpec := range replays {
		replaySpecs = append(replaySpecs, &pb.ReplaySpec{
			Id:        replaySpec.ID.String(),
			JobName:   replaySpec.Job.GetName(),
			StartDate: timestamppb.New(replaySpec.StartDate),
			EndDate:   timestamppb.New(replaySpec.EndDate),
			State:     replaySpec.Status,
			Config:    replaySpec.Config,
			CreatedAt: timestamppb.New(replaySpec.CreatedAt),
		})
	}

	return &pb.ListReplaysResponse{
		ReplayList: replaySpecs,
	}, nil
}

func (sv *RuntimeServiceServer) parseReplayRequest(ctx context.Context, projectName, namespace string,
	jobName, startDate, endDate string, forceFlag bool, allowedDownstreams []string) (models.ReplayRequest, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, projectName, namespace)
	if err != nil {
		return models.ReplayRequest{}, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	jobSpec, err := sv.jobSvc.GetByName(ctx, jobName, namespaceSpec)
	if err != nil {
		return models.ReplayRequest{}, status.Errorf(codes.NotFound, "%s: failed to find the job %s for namespace %s", err.Error(),
			jobName, namespace)
	}

	windowStart, err := time.Parse(job.ReplayDateFormat, startDate)
	if err != nil {
		return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "unable to parse replay start date(e.g. %s): %v", job.ReplayDateFormat, err)
	}

	windowEnd := windowStart
	if endDate != "" {
		if windowEnd, err = time.Parse(job.ReplayDateFormat, endDate); err != nil {
			return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "unable to parse replay end date(e.g. %s): %v", job.ReplayDateFormat, err)
		}
	}
	if windowEnd.Before(windowStart) {
		return models.ReplayRequest{}, status.Errorf(codes.InvalidArgument, "replay end date cannot be before start date")
	}

	replayRequest := models.ReplayRequest{
		Job:                         jobSpec,
		Start:                       windowStart,
		End:                         windowEnd,
		Project:                     namespaceSpec.ProjectSpec,
		Force:                       forceFlag,
		AllowedDownstreamNamespaces: allowedDownstreams,
	}
	return replayRequest, nil
}
