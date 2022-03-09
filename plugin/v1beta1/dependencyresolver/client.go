package dependencyresolver

import (
	"context"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/base"
	"github.com/odpf/optimus/plugin/v1beta1/cli"
)

const (
	PluginGRPCMaxRetry = 3
	BackoffDuration    = 200 * time.Millisecond
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client             pbp.DependencyResolverModServiceClient
	projectSpecAdapter ProjectSpecAdapter

	baseClient *base.GRPCClient

	// plugin name
	name string
}

func (m *GRPCClient) PluginInfo() (*models.PluginInfoResponse, error) {
	return m.baseClient.PluginInfo()
}

func (m *GRPCClient) SetName(n string) {
	m.name = n
}

func (m *GRPCClient) GenerateDestination(ctx context.Context, request models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	spanCtx, span := base.Tracer.Start(ctx, "GenerateDestination")
	defer span.End()
	resp, err := m.client.GenerateDestination(spanCtx, &pbp.GenerateDestinationRequest{
		Config:  cli.AdaptConfigsToProto(request.Config),
		Assets:  cli.AdaptAssetsToProto(request.Assets),
		Project: m.projectSpecAdapter.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.name),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	}, grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.baseClient.MakeFatalOnConnErr(err)
		return nil, err
	}
	return &models.GenerateDestinationResponse{
		Destination: resp.Destination,
		Type:        models.DestinationType(resp.DestinationType),
	}, nil
}

func (m *GRPCClient) GenerateDependencies(ctx context.Context, request models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	spanCtx, span := base.Tracer.Start(ctx, "GenerateDependencies")
	defer span.End()
	resp, err := m.client.GenerateDependencies(spanCtx, &pbp.GenerateDependenciesRequest{
		Config:  cli.AdaptConfigsToProto(request.Config),
		Assets:  cli.AdaptAssetsToProto(request.Assets),
		Project: m.projectSpecAdapter.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.name),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	}, grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.baseClient.MakeFatalOnConnErr(err)
		return nil, err
	}
	return &models.GenerateDependenciesResponse{
		Dependencies: resp.Dependencies,
	}, nil
}
