package base

import (
	"context"
	"fmt"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/models"
)

// GRPCServer will be used by plugins, this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.BasePlugin

	pbp.UnimplementedBaseServiceServer
}

func (s *GRPCServer) PluginInfo(ctx context.Context, req *pbp.PluginInfoRequest) (*pbp.PluginInfoResponse, error) {
	n, err := s.Impl.PluginInfo()
	if err != nil {
		return nil, err
	}

	ptype := pbp.PluginType_PLUGIN_TYPE_HOOK
	switch n.PluginType {
	case models.PluginTypeTask:
		ptype = pbp.PluginType_PLUGIN_TYPE_TASK
	}

	var mtype []pbp.PluginMod
	for _, mod := range n.PluginMods {
		switch mod {
		case models.ModTypeCLI:
			mtype = append(mtype, pbp.PluginMod_PLUGIN_MOD_CLI)
		case models.ModTypeDependencyResolver:
			mtype = append(mtype, pbp.PluginMod_PLUGIN_MOD_DEPENDENCYRESOLVER)
		default:
			return nil, fmt.Errorf("plugin mod is of unknown type: %s", mod)
		}
	}

	htype := pbp.HookType_HOOK_TYPE_UNSPECIFIED
	switch n.HookType {
	case models.HookTypePre:
		htype = pbp.HookType_HOOK_TYPE_PRE
	case models.HookTypePost:
		htype = pbp.HookType_HOOK_TYPE_POST
	case models.HookTypeFail:
		htype = pbp.HookType_HOOK_TYPE_FAIL
	}
	return &pbp.PluginInfoResponse{
		Name:          n.Name,
		PluginType:    ptype,
		PluginMods:    mtype,
		PluginVersion: n.PluginVersion,
		ApiVersion:    n.APIVersion,
		Description:   n.Description,
		Image:         n.Image,
		DependsOn:     n.DependsOn,
		HookType:      htype,
		SecretPath:    n.SecretPath,
	}, nil
}
