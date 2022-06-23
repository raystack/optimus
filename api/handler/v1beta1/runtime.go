package v1beta1

import (
	"context"
	"encoding/json"

	"github.com/odpf/salt/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/utils"
)

type JobEventService interface {
	Register(context.Context, models.NamespaceSpec, models.JobSpec, models.JobEvent) error
}

type RuntimeServiceServer struct {
	version           string
	jobSvc            models.JobService
	jobEventSvc       JobEventService
	namespaceService  service.NamespaceService
	monitoringService service.MonitoringService
	l                 log.Logger
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

	jobEvent := models.JobEvent{
		Type:  models.JobEventType(utils.FromEnumProto(req.GetEvent().Type.String(), "TYPE")),
		Value: eventValues,
	}

	err = sv.monitoringService.ProcessEvent(ctx, jobEvent, namespaceSpec, jobSpec)
	if err != nil {
		jobEventByteString, _ := json.Marshal(jobEvent)
		sv.l.Error("Airflow event not registration ", err.Error(), " event Payload::", string(jobEventByteString))
	}

	if err := sv.jobEventSvc.Register(ctx, namespaceSpec, jobSpec, jobEvent); err != nil {
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
	monitoringService service.MonitoringService,
) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		l:                 l,
		version:           version,
		jobSvc:            jobSvc,
		jobEventSvc:       jobEventService,
		namespaceService:  namespaceService,
		monitoringService: monitoringService,
	}
}
