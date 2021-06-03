package hook

import (
	"context"

	"github.com/odpf/optimus/models"

	"github.com/hashicorp/go-plugin"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"google.golang.org/grpc"
)

var _ plugin.GRPCPlugin = &Plugin{}

type ProjectSpecAdapter interface {
	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification
}

type Plugin struct {
	plugin.NetRPCUnsupportedPlugin
	plugin.GRPCPlugin

	Impl               models.HookPlugin
	ProjectSpecAdapter ProjectSpecAdapter
}

func (p *Plugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterHookPluginServer(s, &GRPCServer{
		Impl:               p.Impl,
		projectSpecAdapter: p.ProjectSpecAdapter,
	})
	return nil
}

func (p *Plugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client:             pb.NewHookPluginClient(c),
		projectSpecAdapter: p.ProjectSpecAdapter,
	}, nil
}
