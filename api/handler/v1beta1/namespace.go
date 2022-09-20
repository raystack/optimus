package v1beta1

import (
	"context"

	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/service"
)

type NamespaceServiceServer struct {
	l                log.Logger
	namespaceService service.NamespaceService
	pb.UnimplementedNamespaceServiceServer
}

func (sv *NamespaceServiceServer) RegisterProjectNamespace(ctx context.Context, req *pb.RegisterProjectNamespaceRequest) (*pb.RegisterProjectNamespaceResponse, error) {
	namespaceSpec := FromNamespaceProto(req.GetNamespace())
	err := sv.namespaceService.Save(ctx, req.GetProjectName(), namespaceSpec)
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to store namespace")
	}

	return &pb.RegisterProjectNamespaceResponse{
		Success: true,
		Message: "saved successfully",
	}, nil
}

func (sv *NamespaceServiceServer) ListProjectNamespaces(ctx context.Context, req *pb.ListProjectNamespacesRequest) (*pb.ListProjectNamespacesResponse, error) {
	namespaceSpecs, err := sv.namespaceService.GetAll(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "not able to list namespaces")
	}

	namespaceSpecsProto := []*pb.NamespaceSpecification{}
	for _, namespace := range namespaceSpecs {
		namespaceSpecsProto = append(namespaceSpecsProto, ToNamespaceProto(namespace))
	}

	return &pb.ListProjectNamespacesResponse{
		Namespaces: namespaceSpecsProto,
	}, nil
}

func (sv *NamespaceServiceServer) GetNamespace(ctx context.Context, request *pb.GetNamespaceRequest) (*pb.GetNamespaceResponse, error) {
	namespace, err := sv.namespaceService.Get(ctx, request.ProjectName, request.NamespaceName)
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}
	return &pb.GetNamespaceResponse{
		Namespace: ToNamespaceProto(namespace),
	}, nil
}

func NewNamespaceServiceServer(l log.Logger, namespaceService service.NamespaceService) *NamespaceServiceServer {
	return &NamespaceServiceServer{
		l:                l,
		namespaceService: namespaceService,
	}
}
