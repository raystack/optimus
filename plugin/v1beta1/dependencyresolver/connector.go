package dependencyresolver

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/base"
	"github.com/odpf/optimus/plugin/v1beta1/cli"
	pbp "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
)

var _ plugin.GRPCPlugin = &Connector{}

type Connector struct {
	plugin.NetRPCUnsupportedPlugin
	plugin.GRPCPlugin

	impl models.DependencyResolverMod

	logger hclog.Logger
}

func (p *Connector) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	pbp.RegisterDependencyResolverModServiceServer(s, &GRPCServer{
		Impl: p.impl,
	})
	return nil
}

func (p *Connector) GRPCClient(_ context.Context, _ *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		client: pbp.NewDependencyResolverModServiceClient(c),
		baseClient: &base.GRPCClient{
			Client: pbp.NewBaseServiceClient(c),
			Logger: p.logger,
		},
	}, nil
}

func NewPlugin(impl models.DependencyResolverMod, logger hclog.Logger) *Connector {
	return &Connector{
		impl:   impl,
		logger: logger,
	}
}

func NewPluginWithAdapter(impl models.DependencyResolverMod, logger hclog.Logger) *Connector {
	return &Connector{
		impl:   impl,
		logger: logger,
	}
}

func NewPluginClient(logger hclog.Logger) *Connector {
	return &Connector{
		logger: logger,
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
		GRPCServer: func(options []grpc.ServerOption) *grpc.Server {
			traceOpt := []grpc.ServerOption{
				grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
				grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
			}
			return plugin.DefaultGRPCServer(append(traceOpt, options...))
		},
		Logger: logger,
	})
}
