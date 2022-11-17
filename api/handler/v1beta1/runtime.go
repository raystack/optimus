package v1beta1

import (
	"context"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type JobEventService interface {
	Register(context.Context, models.NamespaceSpec, models.JobSpec, models.JobEvent) error
}

type RuntimeServiceServer struct {
	version string
	l       log.Logger
	pb.UnimplementedRuntimeServiceServer
}

func (sv *RuntimeServiceServer) Version(_ context.Context, version *pb.VersionRequest) (*pb.VersionResponse, error) { // nolint: unparam
	sv.l.Info("client requested for ping", "version", version.Client)
	response := &pb.VersionResponse{Server: sv.version}
	return response, nil
}

func NewRuntimeServiceServer(l log.Logger, version string) *RuntimeServiceServer {
	return &RuntimeServiceServer{
		l:       l,
		version: version,
	}
}
