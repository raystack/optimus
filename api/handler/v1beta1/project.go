package v1beta1

import (
	"context"
	"fmt"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

func (sv *RuntimeServiceServer) RegisterProject(ctx context.Context, req *pb.RegisterProjectRequest) (*pb.RegisterProjectResponse, error) {
	projectSpec := sv.adapter.FromProjectProto(req.GetProject())
	if err := sv.projectService.Save(ctx, projectSpec); err != nil {
		return nil, mapToGRPCErr(sv.l, err, fmt.Sprintf("not able to register project %s", req.GetProject().Name))
	}

	responseMsg := "project saved successfully."
	if req.Namespace != nil { //nolint:staticcheck
		responseMsg += " ignoring to save namespace (deprecated). please use register namespace rpc."
	}
	return &pb.RegisterProjectResponse{
		Success: true,
		Message: responseMsg,
	}, nil
}

func (sv *RuntimeServiceServer) ListProjects(ctx context.Context, _ *pb.ListProjectsRequest) (*pb.ListProjectsResponse, error) {
	projects, err := sv.projectService.GetAll(ctx)
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "failed to retrieve saved projects")
	}

	projSpecsProto := []*pb.ProjectSpecification{}
	for _, project := range projects {
		projSpecsProto = append(projSpecsProto, sv.adapter.ToProjectProto(project))
	}

	return &pb.ListProjectsResponse{
		Projects: projSpecsProto,
	}, nil
}
