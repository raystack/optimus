package base

import (
	"context"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"

	"github.com/odpf/optimus/models"

	hplugin "github.com/hashicorp/go-plugin"
	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins/v1beta1"
	"google.golang.org/grpc"
)

var _ hplugin.GRPCPlugin = &Connector{}

type Connector struct {
	hplugin.NetRPCUnsupportedPlugin
	hplugin.GRPCPlugin

	impl   models.BasePlugin
	logger hclog.Logger
}

func (p *Connector) GRPCServer(broker *hplugin.GRPCBroker, s *grpc.Server) error {
	pbp.RegisterBaseServiceServer(s, &GRPCServer{
		Impl: p.impl,
	})
	return nil
}

func (p *Connector) GRPCClient(ctx context.Context, broker *hplugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{
		Client: pbp.NewBaseServiceClient(c),
		Logger: p.logger,
	}, nil
}

func NewPlugin(impl interface{}, logger hclog.Logger) *Connector {
	return &Connector{
		impl:   impl.(models.BasePlugin),
		logger: logger,
	}
}

func NewPluginClient(logger hclog.Logger) *Connector {
	return &Connector{
		logger: logger,
	}
}

func Serve(t interface{}, logger hclog.Logger) {
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins: map[string]plugin.Plugin{
			models.PluginTypeBase: NewPlugin(t, logger),
		},
		GRPCServer: plugin.DefaultGRPCServer,
		Logger:     logger,
	})
}
