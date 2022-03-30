package dependencyresolver

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/base"
	"github.com/odpf/optimus/plugin/v1beta1/cli"
)

var _ plugin.GRPCPlugin = &Connector{}

type ProjectSpecAdapter interface {
	FromProjectProtoWithSecrets(*pb.ProjectSpecification) models.ProjectSpec
	ToProjectProtoWithSecret(models.ProjectSpec, models.InstanceType, string) *pb.ProjectSpecification
}

type Connector struct {
	plugin.NetRPCUnsupportedPlugin
	plugin.GRPCPlugin

	impl               models.DependencyResolverMod
	projectSpecAdapter ProjectSpecAdapter

	logger hclog.Logger
}

func (p *Connector) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	pbp.RegisterDependencyResolverModServiceServer(s, &GRPCServer{
		Impl:               p.impl,
		projectSpecAdapter: p.projectSpecAdapter,
	})
	return nil
}

func (p *Connector) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client:             pbp.NewDependencyResolverModServiceClient(c),
		projectSpecAdapter: p.projectSpecAdapter,
		baseClient: &base.GRPCClient{
			Client: pbp.NewBaseServiceClient(c),
			Logger: p.logger,
		},
	}, nil
}

func NewPlugin(impl models.DependencyResolverMod, logger hclog.Logger) *Connector {
	return &Connector{
		impl:               impl,
		projectSpecAdapter: v1.NewAdapter(nil, nil),
		logger:             logger,
	}
}

func NewPluginWithAdapter(impl models.DependencyResolverMod, logger hclog.Logger, projAdapt ProjectSpecAdapter) *Connector {
	return &Connector{
		impl:               impl,
		logger:             logger,
		projectSpecAdapter: projAdapt,
	}
}

func NewPluginClient(logger hclog.Logger) *Connector {
	return &Connector{
		projectSpecAdapter: v1.NewAdapter(nil, nil),
		logger:             logger,
	}
}

func ServeWithCLI(t models.DependencyResolverMod, c models.CommandLineMod, logger hclog.Logger) {
	startServe(map[string]plugin.Plugin{
		models.PluginTypeBase:                     base.NewPlugin(t, logger),
		models.ModTypeCLI.String():                cli.NewPlugin(c, logger),
		models.ModTypeDependencyResolver.String(): NewPlugin(t, logger),
	}, logger)
}

func Serve(t models.DependencyResolverMod, logger hclog.Logger) {
	startServe(map[string]plugin.Plugin{
		models.PluginTypeBase:                     base.NewPlugin(t, logger),
		models.ModTypeDependencyResolver.String(): NewPlugin(t, logger),
	}, logger)
}

func startServe(mp map[string]plugin.Plugin, logger hclog.Logger) {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: base.Handshake,
		Plugins:         mp,
		GRPCServer:      plugin.DefaultGRPCServer,
		Logger:          logger,
	})
}
