package task

import (
	"context"

	v1 "github.com/odpf/optimus/api/handler/v1"

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

func NewPlugin(impl models.TaskPlugin) *Plugin {
	return &Plugin{
		Impl:               impl,
		ProjectSpecAdapter: v1.NewAdapter(nil, nil, nil),
	}
}

func NewPluginWithAdapter(impl models.TaskPlugin, projAdapt ProjectSpecAdapter) *Plugin {
	return &Plugin{
		Impl:               impl,
		ProjectSpecAdapter: projAdapt,
	}
}
