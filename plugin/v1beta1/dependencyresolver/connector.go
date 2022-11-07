package dependencyresolver

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/odpf/optimus/models"
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
		logger: p.logger,
	}, nil
}

func NewPlugin(impl models.DependencyResolverMod, logger hclog.Logger) *Connector {
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

func Serve(t models.DependencyResolverMod, logger hclog.Logger) {
	pluginsMap := map[string]plugin.Plugin{
		models.ModTypeDependencyResolver.String(): NewPlugin(t, logger),
	}
	grpcServer := func(options []grpc.ServerOption) *grpc.Server {
		traceOpt := []grpc.ServerOption{
			grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
			grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
		}
		return plugin.DefaultGRPCServer(append(traceOpt, options...))
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         pluginsMap,
		GRPCServer:      grpcServer,
		Logger:          logger,
	})
}
