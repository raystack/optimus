package yaml

import "github.com/odpf/optimus/sdk/plugin"

const GenericPluginName = "generic"

func GenericPlugin() *PluginSpec {
	return &PluginSpec{
		Info: plugin.Info{
			Name:          GenericPluginName,
			Description:   "plugin to allow running adhoc actions",
			Image:         "<user defined>",
			PluginType:    plugin.TypeHook,
			PluginMods:    []plugin.Mod{plugin.ModTypeCLI},
			PluginVersion: "1.0.0",
			HookType:      plugin.HookTypePost,
		},
	}
}
