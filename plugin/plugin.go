package plugin

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"

	v1 "github.com/odpf/optimus/api/handler/v1"

	"github.com/odpf/optimus/plugin/hook"

	"github.com/odpf/optimus/models"

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

	// Magic values
	// should always remain constant
	MagicCookieKey   = "OP_PLUGIN_MAGIC_COOKIE"
	MagicCookieValue = "ksxR4BqCT81whVF2dVEUpYZXwM3pazSkP4IbVc6f2Kns57ypp2c0z0GzQNMdHSUk"
)

var (
	// Supported plugin types
	TaskPluginName = models.InstanceTypeTask.String()
	HookPluginName = models.InstanceTypeHook.String()
)

func Initialize(pluginLogger hclog.Logger) error {
	discoveredPlugins, err := DiscoverPlugins(pluginLogger)
	if err != nil {
		return errors.Wrap(err, "DiscoverPlugins")
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
		TaskPluginName: &task.Plugin{
			ProjectSpecAdapter: v1.NewAdapter(nil, nil, nil),
		},
		HookPluginName: &hook.Plugin{
			ProjectSpecAdapter: v1.NewAdapter(nil, nil, nil),
		},
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
				return errors.Wrapf(err, "client.Client(): %s", pluginPath)
			}

			// Request the plugin
			raw, err := rpcClient.Dispense(pluginType)
			if err != nil {
				return errors.Wrapf(err, "rpcClient.Dispense: %s", pluginPath)
			}

			switch pluginType {
			case TaskPluginName:
				taskClient := raw.(models.TaskPlugin)
				taskSchema, err := taskClient.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
				if err != nil {
					return errors.Wrapf(err, "taskClient.GetTaskSchema: %s", pluginPath)
				}
				pluginLogger.Debug("tested plugin communication for task", taskSchema.Name)

				{
					// update name, will be later used for filtering secrets
					taskGRPCClient := raw.(*task.GRPCClient)
					taskGRPCClient.Name = taskSchema.Name
				}

				if err := models.TaskRegistry.Add(taskClient); err != nil {
					return errors.Wrapf(err, "models.TaskRegistry.Add: %s", pluginPath)
				}
			case HookPluginName:
				hookClient := raw.(models.HookPlugin)
				hookSchema, err := hookClient.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
				if err != nil {
					return errors.Wrapf(err, "hookClient.GetTaskSchema: %s", pluginPath)
				}
				pluginLogger.Debug("tested plugin communication for hook ", hookSchema.Name)

				if err := models.HookRegistry.Add(hookClient); err != nil {
					return errors.Wrapf(err, "models.HookRegistry.Add: %s", pluginPath)
				}
			default:
				return errors.Wrapf(err, "unsupported plugin type: %s", pluginType)
			}
		}
	}

	return nil
}

// DiscoverPlugins look for plugin binaries in following folders
// order to search is top to down
// ./
// <exec>/
// <exec>/.optimus/plugins
// $HOME/.optimus/plugins
// /usr/bin
// /usr/local/bin
//
// for duplicate binaries(even with different versions for now), only the first found will be used
// sample plugin name: optimus-task-myplugin_0.1_linux_amd64
func DiscoverPlugins(pluginLogger hclog.Logger) (map[string][]string, error) {
	var (
		prefix            = "optimus-"
		suffix            = fmt.Sprintf("_%s_%s", runtime.GOOS, runtime.GOARCH)
		discoveredPlugins = map[string][]string{}
	)

	dirs := []string{}
	// current working directory
	if p, err := os.Getwd(); err == nil {
		dirs = append(dirs, p)
	}
	{
		// look in the same directory as the executable
		if exePath, err := os.Executable(); err != nil {
			pluginLogger.Debug(fmt.Sprintf("Error discovering exe directory: %s", err))
		} else {
			dirs = append(dirs, filepath.Dir(exePath))
		}
	}
	{
		// add user home directory
		if currentHomeDir, err := os.UserHomeDir(); err == nil {
			dirs = append(dirs, filepath.Join(currentHomeDir, ".optimus", "plugins"))
		}
	}
	dirs = append(dirs, []string{"/usr/bin", "/usr/local/bin"}...)

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

			pluginName := strings.Split(nameParts[2], "_")[0]
			absPath = filepath.Clean(absPath)
			switch nameParts[1] {
			case TaskPluginName:
				// check for duplicate binaries, could be different versions
				// if we have already discovered one, ignore rest
				isAlreadyFound := false
				for _, storedName := range discoveredPlugins[TaskPluginName] {
					if strings.Contains(storedName, pluginName) {
						isAlreadyFound = true
					}
				}

				if !isAlreadyFound {
					discoveredPlugins[TaskPluginName] = append(discoveredPlugins[TaskPluginName], absPath)
				}
			case HookPluginName:
				isAlreadyFound := false
				for _, storedName := range discoveredPlugins[HookPluginName] {
					if strings.Contains(storedName, pluginName) {
						isAlreadyFound = true
					}
				}

				if !isAlreadyFound {
					discoveredPlugins[HookPluginName] = append(discoveredPlugins[HookPluginName], absPath)
				}
			default:
				// skip
			}
		}
	}
	return discoveredPlugins, nil
}
