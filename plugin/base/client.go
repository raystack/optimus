package base

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins"
	"github.com/odpf/optimus/models"
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	Client pbp.BaseClient
	Logger hclog.Logger

	// plugin name
	Name string
}

func (m *GRPCClient) PluginInfo() (*models.PluginInfoResponse, error) {
	resp, err := m.Client.PluginInfo(context.Background(), &pbp.PluginInfoRequest{})
	if err != nil {
		m.makeFatal(err)
		return nil, err
	}
	m.Name = resp.Name

	var ptype models.PluginType
	switch resp.PluginType {
	case pbp.PluginType_PluginType_TASK:
		ptype = models.PluginTypeTask
	case pbp.PluginType_PluginType_HOOK:
		ptype = models.PluginTypeHook
	default:
		return nil, fmt.Errorf("plugin is of unknown type: %q", resp.GetPluginType().String())
	}

	var mtype []models.PluginMod
	for _, mod := range resp.PluginMods {
		switch mod {
		case pbp.PluginMod_PluginMod_CLI:
			mtype = append(mtype, models.ModTypeCLI)
		case pbp.PluginMod_PluginMod_DEPENDENCYRESOLVER:
			mtype = append(mtype, models.ModTypeDependencyResolver)
		default:
			return nil, fmt.Errorf("plugin mod is of unknown type: %q", mod.String())
		}
	}

	var htype models.HookType
	switch resp.HookType {
	case pbp.HookType_HookType_PRE:
		htype = models.HookTypePre
	case pbp.HookType_HookType_POST:
		htype = models.HookTypePost
	case pbp.HookType_HookType_FAIL:
		htype = models.HookTypeFail
	default:
		if resp.PluginType == pbp.PluginType_PluginType_HOOK {
			return nil, fmt.Errorf("hook is of unknown type: %q", resp.GetHookType().String())
		}
	}

	return &models.PluginInfoResponse{
		Name:          resp.Name,
		Description:   resp.Description,
		PluginType:    ptype,
		PluginMods:    mtype,
		PluginVersion: resp.PluginVersion,
		APIVersion:    resp.ApiVersion,
		Image:         resp.Image,
		SecretPath:    resp.SecretPath,
		DependsOn:     resp.DependsOn,
		HookType:      htype,
	}, nil
}

func (m *GRPCClient) makeFatal(err error) {
	if strings.Contains(err.Error(), "connection refused") && strings.Contains(err.Error(), "dial unix") {
		m.Logger.Error(fmt.Sprintf("Core communication failed with: \n%s", err.Error()))
	}
	m.Logger.Error(fmt.Sprintf("Exiting application, plugin crashed %s", m.Name))

	// TODO(kush.sharma): once plugins are more stable and we have strict checks
	// we can remove this fail
	os.Exit(1)
}
