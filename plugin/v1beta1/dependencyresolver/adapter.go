package dependencyresolver

import (
	pb "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/sdk/plugin"
)

func adaptConfigsToProto(c plugin.Configs) *pb.Configs {
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

func adaptConfigsFromProto(a *pb.Configs) plugin.Configs {
	tc := plugin.Configs{}
	for _, c := range a.Configs {
		tc = append(tc, plugin.Config{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsToProto(a plugin.Assets) *pb.Assets {
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

func adaptAssetsFromProto(a *pb.Assets) plugin.Assets {
	tc := plugin.Assets{}
	for _, c := range a.Assets {
		tc = append(tc, plugin.Asset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}
