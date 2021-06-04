package task

import (
	"context"

	"github.com/odpf/optimus/models"

	"github.com/hashicorp/go-plugin"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"google.golang.org/grpc"
)

var _ plugin.GRPCPlugin = &Plugin{}

type ProjectSpecAdapter interface {
	FromProjectProtoWithSecrets(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProtoWithSecret(models.ProjectSpec, models.InstanceType, string) *pb.ProjectSpecification
}

type Plugin struct {
	plugin.NetRPCUnsupportedPlugin
	plugin.GRPCPlugin

	Impl               models.TaskPlugin
	ProjectSpecAdapter ProjectSpecAdapter
}

func (p *Plugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterTaskPluginServer(s, &GRPCServer{
		Impl:               p.Impl,
		projectSpecAdapter: p.ProjectSpecAdapter,
	})
	return nil
}

func (p *Plugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client:             pb.NewTaskPluginClient(c),
		projectSpecAdapter: p.ProjectSpecAdapter,
	}, nil
}
