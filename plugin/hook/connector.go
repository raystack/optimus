package hook

import (
	"context"

	v1 "github.com/odpf/optimus/api/handler/v1"

	"github.com/odpf/optimus/models"

	hplugin "github.com/hashicorp/go-plugin"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"google.golang.org/grpc"
)

var _ hplugin.GRPCPlugin = &connector{}

type ProjectSpecAdapter interface {
	FromProjectProto(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProto(models.ProjectSpec) *pb.ProjectSpecification
}

type connector struct {
	hplugin.NetRPCUnsupportedPlugin
	hplugin.GRPCPlugin

	impl               models.HookPlugin
	projectSpecAdapter ProjectSpecAdapter
}

func (p *connector) GRPCServer(broker *hplugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterHookPluginServer(s, &GRPCServer{
		Impl:               p.impl,
		projectSpecAdapter: p.projectSpecAdapter,
	})
	return nil
}

func (p *connector) GRPCClient(ctx context.Context, broker *hplugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client:             pb.NewHookPluginClient(c),
		projectSpecAdapter: p.projectSpecAdapter,
	}, nil
}

func NewPlugin(impl models.HookPlugin) *connector {
	return &connector{
		impl:               impl,
		projectSpecAdapter: v1.NewAdapter(nil, nil, nil),
	}
}

func NewPluginWithAdapter(impl models.HookPlugin, projAdapt ProjectSpecAdapter) *connector {
	return &connector{
		impl:               impl,
		projectSpecAdapter: projAdapt,
	}
}

func NewPluginClient() *connector {
	return &connector{
		projectSpecAdapter: v1.NewAdapter(nil, nil, nil),
	}
}
