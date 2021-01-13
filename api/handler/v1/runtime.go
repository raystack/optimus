package v1

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "github.com/odpf/optimus/api/proto/v1"
	log "github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type ProjectRepoFactory interface {
	New() store.ProjectRepository
}
type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) (models.JobSpec, error)
	ToJobProto(models.JobSpec) *pb.JobSpecification
	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification
}

type runtimeServiceServer struct {
	version            string
	jobSvc             models.JobService
	adapter            ProtoAdapter
	projectRepoFactory ProjectRepoFactory

	pb.UnimplementedRuntimeServiceServer
}

func (sv *runtimeServiceServer) Version(ctx context.Context, version *pb.VersionRequest) (*pb.VersionResponse, error) {
	log.I(fmt.Printf("client with version %s requested for ping ", version.Client))
	response := &pb.VersionResponse{
		Server: sv.version,
	}
	return response, nil
}

func (sv *runtimeServiceServer) DeploySpecification(ctx context.Context, req *pb.DeploySpecificationRequest) (*pb.DeploySpecificationResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(req.GetProject().Name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	adaptJob, err := sv.adapter.FromJobProto(req.GetJob())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = sv.jobSvc.CreateJob(adaptJob, projSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.DeploySpecificationResponse{
		Succcess: true,
	}, nil
}

func (sv *runtimeServiceServer) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	projectRepo := sv.projectRepoFactory.New()
	if err := projectRepo.Save(sv.adapter.FromProjectProto(req.GetProject())); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.RegisterProjectResponse{
		Succcess: true,
		Message:  "saved successfully",
	}, nil
}

func NewRuntimeServiceServer(version string, jobSvc models.JobService, projectRepoFactory ProjectRepoFactory) *runtimeServiceServer {
	return &runtimeServiceServer{
		version:            version,
		jobSvc:             jobSvc,
		adapter:            NewAdapter(),
		projectRepoFactory: projectRepoFactory,
	}
}
