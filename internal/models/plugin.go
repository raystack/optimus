package models

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/odpf/optimus/sdk/plugin"
)

var (
	ErrUnsupportedPlugin = errors.New("unsupported plugin requested, make sure its correctly installed")
)

type PluginRepository struct {
	data       map[string]*plugin.Plugin
	sortedKeys []string
}

func (s *PluginRepository) lazySortPluginKeys() {
	// already sorted
	if len(s.data) == 0 || len(s.sortedKeys) > 0 {
		return
	}

	for k := range s.data {
		s.sortedKeys = append(s.sortedKeys, k)
	}
	sort.Strings(s.sortedKeys)
}

func (s *PluginRepository) GetByName(name string) (*plugin.Plugin, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedPlugin)
}

func (s *PluginRepository) GetAll() []*plugin.Plugin {
	var list []*plugin.Plugin
	s.lazySortPluginKeys() // sorts keys if not sorted
	for _, pluginName := range s.sortedKeys {
		list = append(list, s.data[pluginName])
	}
	return list
}

func (s *PluginRepository) GetTasks() []*plugin.Plugin {
	var list []*plugin.Plugin
	s.lazySortPluginKeys() // sorts keys if not sorted
	for _, pluginName := range s.sortedKeys {
		unit := s.data[pluginName]
		if unit.Info().PluginType == plugin.TypeTask {
			list = append(list, unit)
		}
	}
	return list
}

func (s *PluginRepository) GetHooks() []*plugin.Plugin {
	var list []*plugin.Plugin
	s.lazySortPluginKeys()
	for _, pluginName := range s.sortedKeys {
		unit := s.data[pluginName]
		if unit.Info().PluginType == plugin.TypeHook {
			list = append(list, unit)
		}
	}
	return list
}

func (s *PluginRepository) AddYaml(yamlMod plugin.YamlMod) error {
	info := yamlMod.PluginInfo()
	if err := validateYamlPluginInfo(info); err != nil {
		return err
	}

	if _, ok := s.data[info.Name]; ok {
		// duplicated yaml plugin
		return fmt.Errorf("plugin name already in use %s", info.Name)
	}

	s.data[info.Name] = &plugin.Plugin{YamlMod: yamlMod}
	return nil
}

func (s *PluginRepository) AddBinary(drMod plugin.DependencyResolverMod) error {
	name, err := drMod.GetName(context.Background())
	if err != nil {
		return err
	}

	if plugin, ok := s.data[name]; !ok || plugin.YamlMod == nil {
		// any binary plugin should have its yaml version (for the plugin information)
		return fmt.Errorf("please provide yaml version of the plugin %s", name)
	} else if s.data[name].DependencyMod != nil {
		// duplicated binary plugin
		return fmt.Errorf("plugin name already in use %s", name)
	}

	s.data[name].DependencyMod = drMod
	return nil
}

func validateYamlPluginInfo(info *plugin.Info) error {
	if info.Name == "" {
		return errors.New("plugin name cannot be empty")
	}

	// image is a required field
	if info.Image == "" {
		return errors.New("plugin image cannot be empty")
	}

	// version is a required field
	if info.PluginVersion == "" {
		return errors.New("plugin version cannot be empty")
	}

	// entrypoint is a required field
	if info.Entrypoint == "" {
		return errors.New("entrypoint cannot be empty")
	}

	switch info.PluginType {
	case plugin.TypeTask:
	case plugin.TypeHook:
	default:
		return ErrUnsupportedPlugin
	}

	return nil
}

func NewPluginRepository() *PluginRepository {
	return &PluginRepository{data: map[string]*plugin.Plugin{}}
}
