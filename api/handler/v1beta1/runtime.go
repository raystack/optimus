package v1beta1

import (
	"context"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"

	"github.com/odpf/optimus/core/tree"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type JobEventService interface {
	Register(context.Context, models.NamespaceSpec, models.JobSpec, models.JobEvent) error
}

type ProtoAdapter interface {
	FromJobProto(*pb.JobSpecification) (models.JobSpec, error)
	ToJobProto(models.JobSpec) *pb.JobSpecification
	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification
	FromNamespaceProto(specification *pb.NamespaceSpecification) models.NamespaceSpec
	ToNamespaceProto(spec models.NamespaceSpec) *pb.NamespaceSpecification
	ToInstanceProto(models.InstanceSpec) *pb.InstanceSpec
	FromResourceProto(res *pb.ResourceSpecification, storeName string) (models.ResourceSpec, error)
	ToResourceProto(res models.ResourceSpec) (*pb.ResourceSpecification, error)
	ToReplayExecutionTreeNode(res *tree.TreeNode) (*pb.ReplayExecutionTreeNode, error)
	ToReplayStatusTreeNode(res *tree.TreeNode) (*pb.ReplayStatusTreeNode, error)
}

type RuntimeServiceServer struct {
	version          string
	jobSvc           models.JobService
	jobEventSvc      JobEventService
	namespaceService service.NamespaceService
	l                log.Logger
	pb.UnimplementedRuntimeServiceServer
}

func (sv *RuntimeServiceServer) Version(_ context.Context, version *pb.VersionRequest) (*pb.VersionResponse, error) { // nolint: unparam
	sv.l.Info("client requested for ping", "version", version.Client)
	response := &pb.VersionResponse{
		Server: sv.version,
	}
	return response, nil
}

func (sv *RuntimeServiceServer) RegisterJobEvent(ctx context.Context, req *pb.RegisterJobEventRequest) (*pb.RegisterJobEventResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	jobSpec, err := sv.jobSvc.GetByName(ctx, req.GetJobName(), namespaceSpec)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: failed to find the job %s for namespace %s", err.Error(),
			req.GetJobName(), req.GetNamespaceName())
	}

	if req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing required job event values")
	}

	eventValues := map[string]*structpb.Value{}
	if req.GetEvent().Value != nil {
		eventValues = req.GetEvent().Value.GetFields()
	}
	if err := sv.jobEventSvc.Register(ctx, namespaceSpec, jobSpec, models.JobEvent{
		Type:  models.JobEventType(utils.FromEnumProto(req.GetEvent().Type.String(), "TYPE")),
		Value: eventValues,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register event: \n%s", err.Error())
	}

	return &pb.RegisterJobEventResponse{}, nil
}

func NewRuntimeServiceServer(
	l log.Logger,
	version string,
	jobSvc models.JobService,
	jobEventService JobEventService,
	namespaceService service.NamespaceService,
) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		l:                l,
		version:          version,
		jobSvc:           jobSvc,
		jobEventSvc:      jobEventService,
		namespaceService: namespaceService,
	}
}
