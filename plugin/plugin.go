package plugin

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/odpf/optimus/plugin/hook"

	"github.com/odpf/optimus/models"

	mapset "github.com/deckarep/golang-set"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/odpf/optimus/plugin/task"
)

const (
	// ProtocolVersion is the version that must match between core
	// and plugins. This should be bumped whenever a change happens in
	// one or the other that makes it so that they can't safely communicate.
	// This could be adding a new interface value, methods, etc.
	ProtocolVersion = 1

	// Supported plugin types
	TaskPluginName = "task"
	HookPluginName = "hook"

	// Magic values
	// should always remain constant
	MagicCookieKey   = "OP_PLUGIN_MAGIC_COOKIE"
	MagicCookieValue = "ksxR4BqCT81whVF2dVEUpYZXwM3pazSkP4IbVc6f2Kns57ypp2c0z0GzQNMdHSUk"
)

func Initialize(pluginLogger hclog.Logger) {
	discoveredPlugins, err := DiscoverPlugins()
	if err != nil {
		panic(err)
	}
	pluginLogger.Debug(fmt.Sprintf("discovering plugins(%d)...",
		len(discoveredPlugins[TaskPluginName])+len(discoveredPlugins[HookPluginName])))

	// handshakeConfigs are used to just do a basic handshake between
	// a plugin and host. If the handshake fails, a user friendly error is shown.
	// This prevents users from executing bad plugins or executing a plugin
	// directory. It is a UX feature, not a security feature.
	var handshakeConfig = plugin.HandshakeConfig{
		ProtocolVersion:  ProtocolVersion,
		MagicCookieKey:   MagicCookieKey,
		MagicCookieValue: MagicCookieValue,
	}

	// pluginMap is the map of plugins we can dispense.
	var pluginMap = map[string]plugin.Plugin{
		TaskPluginName: &task.Plugin{},
		HookPluginName: &hook.Plugin{},
	}

	for pluginType, pluginPaths := range discoveredPlugins {
		for _, pluginPath := range pluginPaths {
			// We're a host! Start by launching the plugin process.
			client := plugin.NewClient(&plugin.ClientConfig{
				HandshakeConfig:  handshakeConfig,
				Plugins:          pluginMap,
				Cmd:              exec.Command(pluginPath),
				Managed:          true,
				AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
				Logger:           pluginLogger,
			})

			// Connect via GRPC
			rpcClient, err := client.Client()
			if err != nil {
				pluginLogger.Error("Error:", err.Error())
				os.Exit(1)
			}

			// Request the plugin
			raw, err := rpcClient.Dispense(pluginType)
			if err != nil {
				pluginLogger.Error("Error:", err.Error())
				os.Exit(1)
			}

			switch pluginType {
			case TaskPluginName:
				taskClient := raw.(models.TaskPlugin)
				taskSchema, err := taskClient.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
				if err != nil {
					pluginLogger.Error("Error:", err.Error())
					os.Exit(1)
				}
				pluginLogger.Debug("tested plugin communication for task", taskSchema.Name)

				if err := models.TaskRegistry.Add(taskClient); err != nil {
					pluginLogger.Error("Error:", err.Error())
					os.Exit(1)
				}
			case HookPluginName:
				hookClient := raw.(models.HookPlugin)
				hookSchema, err := hookClient.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
				if err != nil {
					pluginLogger.Error("Error:", err.Error())
					os.Exit(1)
				}
				pluginLogger.Debug("tested plugin communication for hook ", hookSchema.Name)

				if err := models.HookRegistry.Add(hookClient); err != nil {
					pluginLogger.Error("Error:", err.Error())
					os.Exit(1)
				}
			default:
				pluginLogger.Error("Error: unsupported plugin type")
				os.Exit(1)
			}
		}
	}
}

// DiscoverPlugins
// following folders will be used
// ./
// exec/.optimus/plugins
// $HOME/.optimus/plugins
// sample plugin name: optimus-task-myplugin_0.1_linux_amd64
func DiscoverPlugins() (map[string][]string, error) {
	var (
		prefix            = "optimus-"
		suffix            = fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH)
		discoveredPlugins = map[string]mapset.Set{}
	)

	dirs := []string{"."}
	if currentHomeDir, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(currentHomeDir, ".optimus", "plugins"))
	}

	{
		// look in the same directory as the executable
		exePath, err := os.Executable()
		if err != nil {
			log.Printf("[ERROR] Error discovering exe directory: %s", err)
		} else {
			dirs = append(dirs, filepath.Dir(exePath))
		}
		dirs = append(dirs, exePath)
		dirs = append(dirs, filepath.Join(exePath, "dist"))
	}

	for _, dirPath := range dirs {
		fileInfos, err := ioutil.ReadDir(dirPath)
		if err != nil {
			continue
		}

		for _, item := range fileInfos {
			fullName := item.Name()

			if !strings.HasPrefix(fullName, prefix) {
				continue
			}
			if !strings.HasSuffix(fullName, suffix) {
				continue
			}

			absPath, err := filepath.Abs(filepath.Join(dirPath, fullName))
			if err != nil {
				continue
			}

			info, err := os.Stat(absPath)
			if err != nil {
				continue
			}
			if info.IsDir() {
				continue
			}

			// get plugin type
			nameParts := strings.Split(fullName, "-")
			if len(nameParts) < 3 {
				continue
			}

			absPath = filepath.Clean(absPath)
			switch nameParts[1] {
			case TaskPluginName:
				if _, ok := discoveredPlugins[TaskPluginName]; !ok {
					discoveredPlugins[TaskPluginName] = mapset.NewSet()
				}
				discoveredPlugins[TaskPluginName].Add(absPath)
			case HookPluginName:
				if _, ok := discoveredPlugins[HookPluginName]; !ok {
					discoveredPlugins[HookPluginName] = mapset.NewSet()
				}
				discoveredPlugins[HookPluginName].Add(absPath)
			default:
				// skip
			}
		}
	}

	stringMap := map[string][]string{}
	for t, s := range discoveredPlugins {
		for _, path := range s.ToSlice() {
			stringMap[t] = append(stringMap[t], path.(string))
		}
	}
	return stringMap, nil
}
