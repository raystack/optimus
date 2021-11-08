package dependencyresolver

import (
	"context"

	"github.com/odpf/optimus/plugin/v1/cli"

	"github.com/odpf/optimus/models"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins/v1beta1"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.DependencyResolverMod

	projectSpecAdapter ProjectSpecAdapter
	pbp.UnimplementedDependencyResolverModServiceServer
}

func (s *GRPCServer) GenerateDestination(ctx context.Context, req *pbp.GenerateDestinationRequest) (*pbp.GenerateDestinationResponse, error) {
	resp, err := s.Impl.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config:        cli.AdaptConfigsFromProto(req.Config),
		Assets:        cli.AdaptAssetsFromProto(req.Assets),
		Project:       s.projectSpecAdapter.FromProjectProtoWithSecrets(req.Project),
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
		Project:       s.projectSpecAdapter.FromProjectProtoWithSecrets(req.Project),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDependenciesResponse{Dependencies: resp.Dependencies}, nil
}
