package v1beta1

import (
	"context"
	"github.com/odpf/optimus/service"
	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
)

type NamespaceServiceServer struct {
	l                log.Logger
	adapter          ProtoAdapter
	namespaceService service.NamespaceService
	pb.UnimplementedNamespaceServiceServer
}
func (sv *NamespaceServiceServer) RegisterProjectNamespace(ctx context.Context, req *pb.RegisterProjectNamespaceRequest) (*pb.RegisterProjectNamespaceResponse, error) {
	namespaceSpec := sv.adapter.FromNamespaceProto(req.GetNamespace())
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
		namespaceSpecsProto = append(namespaceSpecsProto, sv.adapter.ToNamespaceProto(namespace))
	}

	return &pb.ListProjectNamespacesResponse{
		Namespaces: namespaceSpecsProto,
	}, nil
}

func NewNamespaceServiceServer(l log.Logger, adapter ProtoAdapter, namespaceService service.NamespaceService) *NamespaceServiceServer {
	return &NamespaceServiceServer{
		l:                l,
		adapter:          adapter,
		namespaceService: namespaceService,
	}
}