package v1beta1

import (
	"context"
	"fmt"

	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/service"
)

type ProjectServiceServer struct {
	l              log.Logger
	projectService service.ProjectService
	pb.UnimplementedProjectServiceServer
}

func (sv *ProjectServiceServer) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	projectSpec := FromProjectProto(req.GetProject())
	if err := sv.projectService.Save(ctx, projectSpec); err != nil {
		return nil, mapToGRPCErr(sv.l, err, fmt.Sprintf("not able to register project %s", req.GetProject().Name))
	}

	responseMsg := "project saved successfully."
	if req.Namespace != nil { // nolint:staticcheck
		responseMsg += " ignoring to save namespace (deprecated). please use register namespace rpc."
	}
	return &pb.RegisterProjectResponse{
		Success: true,
		Message: responseMsg,
	}, nil
}

func (sv *ProjectServiceServer) ListProjects(ctx context.Context, _ *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	projects, err := sv.projectService.GetAll(ctx)
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "failed to retrieve saved projects")
	}

	projSpecsProto := []*pb.ProjectSpecification{}
	for _, project := range projects {
		projSpecsProto = append(projSpecsProto, ToProjectProto(project))
	}

	return &pb.ListProjectsResponse{
		Projects: projSpecsProto,
	}, nil
}

func (sv *ProjectServiceServer) GetProject(ctx context.Context, req *pb.GetProjectRequest) (*pb.GetProjectResponse, error) {
	projectSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, fmt.Sprintf("failed to retrieve project [%s]", req.GetProjectName()))
	}
	return &pb.GetProjectResponse{
		Project: ToProjectProto(projectSpec),
	}, nil
}

func NewProjectServiceServer(l log.Logger, projectService service.ProjectService) *ProjectServiceServer {
	return &ProjectServiceServer{
		l:              l,
		projectService: projectService,
	}
}
