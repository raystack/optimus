package task

import (
	"context"

	"github.com/odpf/optimus/models"

	v1 "github.com/odpf/optimus/api/handler/v1"

	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
)

var _ plugin.GRPCPlugin = &Plugin{}

type Plugin struct {
	plugin.NetRPCUnsupportedPlugin
	plugin.GRPCPlugin

	Impl models.TaskPlugin
}

func (p *Plugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pb.RegisterTaskPluginServer(s, &GRPCServer{
		Impl:               p.Impl,
		projectSpecAdapter: v1.NewAdapter(nil, nil, nil),
	})
	return nil
}

func (p *Plugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client:             pb.NewTaskPluginClient(c),
		projectSpecAdapter: v1.NewAdapter(nil, nil, nil),
	}, nil
}
