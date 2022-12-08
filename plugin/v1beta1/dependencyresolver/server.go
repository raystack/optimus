package dependencyresolver

import (
	"context"

	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/internal/utils"
	pbp "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.DependencyResolverMod

	pbp.UnimplementedDependencyResolverModServiceServer
}

func (s *GRPCServer) GetName(ctx context.Context, _ *pbp.GetNameRequest) (*pbp.GetNameResponse, error) {
	name, err := s.Impl.GetName(ctx)
	if err != nil {
		return nil, err
	}
	return &pbp.GetNameResponse{Name: name}, nil
}

func (s *GRPCServer) GenerateDestination(ctx context.Context, req *pbp.GenerateDestinationRequest) (*pbp.GenerateDestinationResponse, error) {
	resp, err := s.Impl.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config:        adaptConfigsFromProto(req.Config),
		Assets:        adaptAssetsFromProto(req.Assets),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDestinationResponse{Destination: resp.Destination, DestinationType: resp.Type.String()}, nil
}

func (s *GRPCServer) GenerateDependencies(ctx context.Context, req *pbp.GenerateDependenciesRequest) (*pbp.GenerateDependenciesResponse, error) {
	resp, err := s.Impl.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
		Config:        adaptConfigsFromProto(req.Config),
		Assets:        adaptAssetsFromProto(req.Assets),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDependenciesResponse{Dependencies: resp.Dependencies}, nil
}

func (s *GRPCServer) CompileAssets(ctx context.Context, req *pbp.CompileAssetsRequest) (*pbp.CompileAssetsResponse, error) {
	var instanceData []models.JobRunSpecData
	for _, inst := range req.InstanceData {
		instanceData = append(instanceData, models.JobRunSpecData{
			Name:  inst.Name,
			Value: inst.Value,
			Type:  utils.FromEnumProto(inst.Type.String(), "type"),
		})
	}

	resp, err := s.Impl.CompileAssets(ctx, models.CompileAssetsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Config:        adaptConfigsFromProto(req.Configs),
		Assets:        adaptAssetsFromProto(req.Assets),
		InstanceData:  instanceData,
		StartTime:     req.StartTime.AsTime(),
		EndTime:       req.EndTime.AsTime(),
	})
	if err != nil {
		return nil, err
	}
	return &pbp.CompileAssetsResponse{
		Assets: adaptAssetsToProto(resp.Assets),
	}, nil
}
