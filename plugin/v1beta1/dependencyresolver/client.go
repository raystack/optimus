package dependencyresolver

import (
	"context"
	"fmt"
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/hashicorp/go-hclog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/internal/utils"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	pbp "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/sdk/plugin"
)

const (
	PluginGRPCMaxRetry = 3
	BackoffDuration    = 200 * time.Millisecond
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client pbp.DependencyResolverModServiceClient
	logger hclog.Logger
}

func (m *GRPCClient) GetName(ctx context.Context) (string, error) {
	spanCtx, span := tracer.Start(ctx, "GetName")
	defer span.End()

	outCtx := propagateMetadata(spanCtx)
	resp, err := m.client.GetName(outCtx,
		&pbp.GetNameRequest{},
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.makeFatalOnConnErr(err)
		return "", err
	}
	return resp.GetName(), nil
}

func (m *GRPCClient) GenerateDestination(ctx context.Context, request plugin.GenerateDestinationRequest) (*plugin.GenerateDestinationResponse, error) {
	spanCtx, span := tracer.Start(ctx, "GenerateDestination")
	defer span.End()

	outCtx := propagateMetadata(spanCtx)
	resp, err := m.client.GenerateDestination(outCtx, &pbp.GenerateDestinationRequest{
		Config:  adaptConfigsToProto(request.Config),
		Assets:  adaptAssetsToProto(request.Assets),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	}, grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.makeFatalOnConnErr(err)
		return nil, err
	}
	return &plugin.GenerateDestinationResponse{
		Destination: resp.Destination,
		Type:        resp.DestinationType,
	}, nil
}

func (m *GRPCClient) GenerateDependencies(ctx context.Context, request plugin.GenerateDependenciesRequest) (*plugin.GenerateDependenciesResponse, error) {
	spanCtx, span := tracer.Start(ctx, "GenerateDependencies")
	defer span.End()

	outCtx := propagateMetadata(spanCtx)
	resp, err := m.client.GenerateDependencies(outCtx, &pbp.GenerateDependenciesRequest{
		Config:  adaptConfigsToProto(request.Config),
		Assets:  adaptAssetsToProto(request.Assets),
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	}, grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.makeFatalOnConnErr(err)
		return nil, err
	}
	return &plugin.GenerateDependenciesResponse{
		Dependencies: resp.Dependencies,
	}, nil
}

func (m *GRPCClient) CompileAssets(ctx context.Context, request plugin.CompileAssetsRequest) (*plugin.CompileAssetsResponse, error) { //nolint: gocritic
	_, span := tracer.Start(ctx, "CompileAssets")
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
		Configs:      adaptConfigsToProto(request.Config),
		Assets:       adaptAssetsToProto(request.Assets),
		InstanceData: instanceData,
		Options:      &pbp.PluginOptions{DryRun: request.DryRun},
		StartTime:    timestamppb.New(request.StartTime),
		EndTime:      timestamppb.New(request.EndTime),
	}, grpc_retry.WithBackoff(grpc_retry.BackoffExponential(BackoffDuration)),
		grpc_retry.WithMax(PluginGRPCMaxRetry))
	if err != nil {
		m.makeFatalOnConnErr(err)
		return nil, err
	}
	return &plugin.CompileAssetsResponse{
		Assets: adaptAssetsFromProto(resp.Assets),
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

func (m *GRPCClient) makeFatalOnConnErr(err error) {
	if !(strings.Contains(err.Error(), "connection refused") && strings.Contains(err.Error(), "dial unix")) {
		return
	}
	m.logger.Error(fmt.Sprintf("Core communication failed with plugin: \n%+v", err))
	m.logger.Error("Exiting application, plugin crashed")
}
