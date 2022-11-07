package dependencyresolver

import (
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
)

func adaptConfigsToProto(c models.PluginConfigs) *pb.Configs {
	tc := &pb.Configs{
		Configs: []*pb.Configs_Config{},
	}
	for _, c := range c {
		tc.Configs = append(tc.Configs, &pb.Configs_Config{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptConfigsFromProto(a *pb.Configs) models.PluginConfigs {
	tc := models.PluginConfigs{}
	for _, c := range a.Configs {
		tc = append(tc, models.PluginConfig{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsToProto(a models.PluginAssets) *pb.Assets {
	tc := &pb.Assets{
		Assets: []*pb.Assets_Asset{},
	}
	for _, c := range a {
		tc.Assets = append(tc.Assets, &pb.Assets_Asset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsFromProto(a *pb.Assets) models.PluginAssets {
	tc := models.PluginAssets{}
	for _, c := range a.Assets {
		tc = append(tc, models.PluginAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}
