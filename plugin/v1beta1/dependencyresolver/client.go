package dependencyresolver

import (
	"context"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/base"
	"github.com/odpf/optimus/plugin/v1beta1/cli"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	pbp "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
)

const (
	PluginGRPCMaxRetry = 3
	BackoffDuration    = 200 * time.Millisecond
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client pbp.DependencyResolverModServiceClient

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

	outCtx := propagateMetadata(spanCtx)
	resp, err := m.client.GenerateDestination(outCtx, &pbp.GenerateDestinationRequest{
		Config:  cli.AdaptConfigsToProto(request.Config),
		Assets:  cli.AdaptAssetsToProto(request.Assets),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
		// Fallback for secrets, please do not remove until secrets cleanup
		Project: v1.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.name),
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

	outCtx := propagateMetadata(spanCtx)
	resp, err := m.client.GenerateDependencies(outCtx, &pbp.GenerateDependenciesRequest{
		Config:  cli.AdaptConfigsToProto(request.Config),
		Assets:  cli.AdaptAssetsToProto(request.Assets),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
		// Fallback for secrets, please do not remove until secrets cleanup
		Project: v1.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.name),
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

func (m *GRPCClient) CompileAssets(ctx context.Context, request models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	_, span := base.Tracer.Start(ctx, "CompileAssets")
	defer span.End()

	var instanceData []*pb.InstanceSpecData
	for _, inst := range request.InstanceData {
		instanceData = append(instanceData, &pb.InstanceSpecData{
			Name:  inst.Name,
			Value: inst.Value,
			Type:  pb.InstanceSpecData_Type(pb.InstanceSpecData_Type_value[utils.ToEnumProto(inst.Type, "type")]),
		})
	}

	resp, err := m.client.CompileAssets(ctx, &pbp.CompileAssetsRequest{
		Configs:      cli.AdaptConfigsToProto(request.Config),
		Assets:       cli.AdaptAssetsToProto(request.Assets),
		InstanceData: instanceData,
		Options:      &pbp.PluginOptions{DryRun: request.DryRun},
		StartTime:    timestamppb.New(request.StartTime),
		EndTime:      timestamppb.New(request.EndTime),
	}, grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.baseClient.MakeFatalOnConnErr(err)
		return nil, err
	}
	return &models.CompileAssetsResponse{
		Assets: cli.AdaptAssetsFromProto(resp.Assets),
	}, nil
}

// propagateMetadata is based on UnaryClientInterceptor, here we cannot use interceptor as it is not
// available as a callOption for the grpc call. We need to manually inject the metadata to context
// https://github.com/open-telemetry/opentelemetry-go-contrib/blob/main/instrumentation/google.golang.org/grpc/otelgrpc/interceptor.go#L67
func propagateMetadata(ctx context.Context) context.Context {
	requestMetadata, _ := metadata.FromOutgoingContext(ctx)
	metadataCopy := requestMetadata.Copy()

	otelgrpc.Inject(ctx, &metadataCopy)
	outgoingCtx := metadata.NewOutgoingContext(ctx, metadataCopy)

	return outgoingCtx
}
