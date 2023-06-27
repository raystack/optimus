package dependencyresolver

import (
	"context"

	"github.com/goto/optimus/internal/utils"
	pbp "github.com/goto/optimus/protos/gotocompany/optimus/plugins/v1beta1"
	"github.com/goto/optimus/sdk/plugin"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl plugin.DependencyResolverMod

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
	resp, err := s.Impl.GenerateDestination(ctx, plugin.GenerateDestinationRequest{
		Config:  adaptConfigsFromProto(req.Config),
		Assets:  adaptAssetsFromProto(req.Assets),
		Options: plugin.Options{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDestinationResponse{Destination: resp.Destination, DestinationType: resp.Type}, nil
}

func (s *GRPCServer) GenerateDependencies(ctx context.Context, req *pbp.GenerateDependenciesRequest) (*pbp.GenerateDependenciesResponse, error) {
	resp, err := s.Impl.GenerateDependencies(ctx, plugin.GenerateDependenciesRequest{
		Config:  adaptConfigsFromProto(req.Config),
		Assets:  adaptAssetsFromProto(req.Assets),
		Options: plugin.Options{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.GenerateDependenciesResponse{Dependencies: resp.Dependencies}, nil
}

func (s *GRPCServer) CompileAssets(ctx context.Context, req *pbp.CompileAssetsRequest) (*pbp.CompileAssetsResponse, error) {
	var instanceData []plugin.JobRunSpecData
	for _, inst := range req.InstanceData {
		instanceData = append(instanceData, plugin.JobRunSpecData{
			Name:  inst.Name,
			Value: inst.Value,
			Type:  utils.FromEnumProto(inst.Type.String(), "type"),
		})
	}

	resp, err := s.Impl.CompileAssets(ctx, plugin.CompileAssetsRequest{
		Options:      plugin.Options{DryRun: req.Options.DryRun},
		Config:       adaptConfigsFromProto(req.Configs),
		Assets:       adaptAssetsFromProto(req.Assets),
		InstanceData: instanceData,
		StartTime:    req.StartTime.AsTime(),
		EndTime:      req.EndTime.AsTime(),
	})
	if err != nil {
		return nil, err
	}
	return &pbp.CompileAssetsResponse{
		Assets: adaptAssetsToProto(resp.Assets),
	}, nil
}
