package v1

import (
	"context"

	pb "github.com/odpf/optimus/api/proto/v1"
	log "github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/models"
)

type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) models.JobSpec
	ToJobProto(models.JobSpec) *pb.JobSpecification
	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification
}

type runtimeServiceServer struct {
	version string
	jobSvc  models.JobService
	adapter ProtoAdapter

	pb.UnimplementedRuntimeServiceServer
}

func (sv *runtimeServiceServer) Ping(ctx context.Context, version *pb.VersionRequest) (*pb.VersionResponse, error) {
	log.I("client with version %s requested for ping", version.Client)
	response := &pb.VersionResponse{
		Server: sv.version,
	}
	return response, nil
}

func (sv *runtimeServiceServer) DeploySpecifications(req *pb.DeploySpecificationRequest) error {
	return sv.jobSvc.CreateJob(sv.adapter.FromJobProto(req.GetJob()), sv.adapter.FromProjectProto(req.GetProject()))
}

func NewRuntimeServiceServer(version string, jobSvc models.JobService) *runtimeServiceServer {
	return &runtimeServiceServer{
		version: version,
		jobSvc:  jobSvc,
		adapter: NewAdapter(),
	}
}
