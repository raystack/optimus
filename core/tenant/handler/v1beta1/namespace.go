package v1beta1

import (
	"context"
	"strings"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type NamespaceService interface {
	Save(ctx context.Context, namespace *tenant.Namespace) error
	Get(context.Context, tenant.ProjectName, tenant.NamespaceName) (*tenant.Namespace, error)
	GetAll(context.Context, tenant.ProjectName) ([]*tenant.Namespace, error)
}

type NamespaceHandler struct {
	l         log.Logger
	nsService NamespaceService

	pb.UnimplementedNamespaceServiceServer
}

func (nh *NamespaceHandler) RegisterProjectNamespace(ctx context.Context, req *pb.RegisterProjectNamespaceRequest) (
	*pb.RegisterProjectNamespaceResponse, error,
) {
	projName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "error in register namespace "+req.GetNamespace().Name)
	}

	namespace, err := fromNamespaceProto(req.GetNamespace(), projName)
	if err != nil {
		return nil, errors.GRPCErr(err, "error in register namespace "+req.GetNamespace().Name)
	}

	err = nh.nsService.Save(ctx, namespace)
	if err != nil {
		return nil, errors.GRPCErr(err, "error in register namespace "+req.GetNamespace().Name)
	}

	return &pb.RegisterProjectNamespaceResponse{}, nil
}

func (nh *NamespaceHandler) ListProjectNamespaces(ctx context.Context, req *pb.ListProjectNamespacesRequest) (
	*pb.ListProjectNamespacesResponse, error,
) {
	projName, err := tenant.ProjectNameFrom(req.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "error in list namespaces")
	}

	namespaces, err := nh.nsService.GetAll(ctx, projName)
	if err != nil {
		return nil, errors.GRPCErr(err, "error in list namespaces")
	}

	var namespaceSpecsProto []*pb.NamespaceSpecification
	for _, namespace := range namespaces {
		namespaceSpecsProto = append(namespaceSpecsProto, toNamespaceProto(namespace))
	}

	return &pb.ListProjectNamespacesResponse{
		Namespaces: namespaceSpecsProto,
	}, nil
}

func (nh *NamespaceHandler) GetNamespace(ctx context.Context, request *pb.GetNamespaceRequest) (
	*pb.GetNamespaceResponse, error,
) {
	projName, err := tenant.ProjectNameFrom(request.GetProjectName())
	if err != nil {
		return nil, errors.GRPCErr(err, "error in get namespace "+request.NamespaceName)
	}

	namespaceName, err := tenant.NamespaceNameFrom(request.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "error in get namespace "+request.NamespaceName)
	}

	namespace, err := nh.nsService.Get(ctx, projName, namespaceName)
	if err != nil {
		return nil, errors.GRPCErr(err, "error in get namespace "+request.NamespaceName)
	}

	return &pb.GetNamespaceResponse{
		Namespace: toNamespaceProto(namespace),
	}, nil
}

func NewNamespaceHandler(l log.Logger, nsService NamespaceService) *NamespaceHandler {
	return &NamespaceHandler{
		l:         l,
		nsService: nsService,
	}
}

func fromNamespaceProto(conf *pb.NamespaceSpecification, projName tenant.ProjectName) (*tenant.Namespace, error) {
	namespaceConf := map[string]string{}
	for key, val := range conf.GetConfig() {
		namespaceConf[strings.ToUpper(key)] = val
	}

	return tenant.NewNamespace(conf.GetName(), projName, namespaceConf)
}

func toNamespaceProto(ns *tenant.Namespace) *pb.NamespaceSpecification {
	return &pb.NamespaceSpecification{
		Name:   ns.Name().String(),
		Config: ns.GetConfigs(),
	}
}
