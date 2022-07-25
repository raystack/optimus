package dependencyresolver

import (
	"context"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/cli"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.DependencyResolverMod

	pbp.UnimplementedDependencyResolverModServiceServer
}

func (s *GRPCServer) GenerateDestination(ctx context.Context, req *pbp.GenerateDestinationRequest) (*pbp.GenerateDestinationResponse, error) {
	resp, err := s.Impl.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config:        cli.AdaptConfigsFromProto(req.Config),
		Assets:        cli.AdaptAssetsFromProto(req.Assets),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDestinationResponse{Destination: resp.Destination, DestinationType: resp.Type.String()}, nil
}

func (s *GRPCServer) GenerateDependencies(ctx context.Context, req *pbp.GenerateDependenciesRequest) (*pbp.GenerateDependenciesResponse, error) {
	resp, err := s.Impl.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
		Config:        cli.AdaptConfigsFromProto(req.Config),
		Assets:        cli.AdaptAssetsFromProto(req.Assets),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDependenciesResponse{Dependencies: resp.Dependencies}, nil
}
