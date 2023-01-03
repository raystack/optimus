package plugin

import (
	"strings"
)

const (
	TypeTask Type = "task"
	TypeHook Type = "hook"

	HookTypePre  HookType = "pre"
	HookTypePost HookType = "post"
	HookTypeFail HookType = "fail"

	ModTypeCLI                Mod = "cli"
	ModTypeDependencyResolver Mod = "dependencyresolver"

	DestinationURNFormat = "%s://%s"
)

type Type string

func (t Type) String() string {
	return string(t)
}

type HookType string

func (ht HookType) String() string {
	return string(ht)
}

type Mod string

func (m Mod) String() string {
	return string(m)
}

type Info struct {
	// Name should as simple as possible with no special characters
	// should start with a character, better if all lowercase
	Name        string
	Description string
	PluginType  Type  `yaml:",omitempty"`
	PluginMods  []Mod `yaml:",omitempty"`

	PluginVersion string   `yaml:",omitempty"`
	APIVersion    []string `yaml:",omitempty"`

	// Image is the full path to docker container that will be scheduled for execution
	Image string

	// DependsOn returns list of hooks this should be executed after
	DependsOn []string `yaml:",omitempty"`

	// PluginType provides the place of execution, could be before the transformation
	// after the transformation, etc
	HookType HookType `yaml:",omitempty"`
}

type YamlMod interface {
	PluginInfo() *Info
	CommandLineMod
}

type Config struct {
	Name  string
	Value string
}

type Configs []Config

func (c Configs) Get(name string) (Config, bool) {
	for _, con := range c {
		if strings.EqualFold(con.Name, name) {
			return con, true
		}
	}
	return Config{}, false
}

func ConfigsFromMap(configMap map[string]string) Configs {
	taskPluginConfigs := Configs{}
	for key, value := range configMap {
		taskPluginConfigs = append(taskPluginConfigs, Config{
			Name:  key,
			Value: value,
		})
	}
	return taskPluginConfigs
}

type Asset struct {
	Name  string
	Value string
}

type Assets []Asset

func AssetsFromMap(assetsMap map[string]string) Assets {
	taskPluginAssets := Assets{}
	for key, value := range assetsMap {
		taskPluginAssets = append(taskPluginAssets, Asset{
			Name:  key,
			Value: value,
		})
	}
	return taskPluginAssets
}

func (c Assets) ToMap() map[string]string {
	mapping := map[string]string{}
	for _, asset := range c {
		mapping[asset.Name] = asset.Value
	}
	return mapping
}

// Plugin is an extensible module implemented outside the core optimus boundaries
type Plugin struct {
	// Mods apply multiple modifications to existing registered plugins which
	// can be used in different circumstances
	DependencyMod DependencyResolverMod
	YamlMod       YamlMod
}

func (p *Plugin) IsYamlPlugin() bool {
	return p.YamlMod != nil
}

func (p *Plugin) GetSurveyMod() CommandLineMod {
	return p.YamlMod
}

func (p *Plugin) Info() *Info {
	if p.YamlMod != nil {
		return p.YamlMod.PluginInfo()
	}
	return nil
}
