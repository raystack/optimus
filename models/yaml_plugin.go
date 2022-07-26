package models

import (
	"context"
)

// var (
// 	YamlPluginRegistry IYamlPluginRepository = newYamlPluginRepository() // singleton
// )

// func newYamlPluginRepository() *yamlPluginsRepo {
// 	return &yamlPluginsRepo{data: map[string]*YamlPlugin{}}
// }

// TODO: remove this, moved to PluginRepository
// type IYamlPluginRepository interface {
// 	Add(*YamlPlugin) error
// 	GetByName(string) (*YamlPlugin, error)
// 	GetAll() []*YamlPlugin
// 	GetTasks() []*YamlPlugin
// 	GetHooks() []*YamlPlugin
// 	PrintAllPlugins(log.Logger)
// }

type YamlQuestions struct {
	Questions []YamlQuestion
}

// type YamlQuestions []YamlQuestion
type YamlQuestion struct {
	Name         string
	Prompt       string
	Help         string
	Default      string
	Multiselect  []string
	SubQuestions []YamlSubQuestion

	Regexp          string
	ValidationError string
	MinLength       int
	MaxLength       int
	Required        bool
}

type YamlSubQuestion struct {
	IfValue   string
	Questions []YamlQuestion
}

// YamlPlugin: Implements CommandLineMod
type YamlPlugin struct {
	Info            *PluginInfoResponse
	YamlQuestions   *YamlQuestions
	PluginQuestions *GetQuestionsResponse
}

func (p *YamlPlugin) PluginInfo() (*PluginInfoResponse, error) {
	return &PluginInfoResponse{
		Name:          p.Info.Name,
		Description:   p.Info.Description,
		Image:         p.Info.Image,
		DependsOn:     p.Info.DependsOn,
		PluginType:    p.Info.PluginType,
		PluginVersion: p.Info.PluginVersion,
		HookType:      p.Info.HookType,
		PluginMods:    p.Info.PluginMods,
	}, nil
}

func (p *YamlPlugin) GetQuestions(context.Context, GetQuestionsRequest) (*GetQuestionsResponse, error) {
	return p.PluginQuestions, nil
}

func (p *YamlPlugin) ValidateQuestion(context.Context, ValidateQuestionRequest) (*ValidateQuestionResponse, error) {
	// TODO :
	return &ValidateQuestionResponse{
		Success: true,
	}, nil
}

// to be depricated,
// Implement config infrerence from config hierarchy/inheritence
func (p *YamlPlugin) DefaultConfig(context.Context, DefaultConfigRequest) (*DefaultConfigResponse, error) {

	return &DefaultConfigResponse{
		Config: []PluginConfig{},
	}, nil
}

func (p *YamlPlugin) DefaultAssets(context.Context, DefaultAssetsRequest) (*DefaultAssetsResponse, error) {

	return &DefaultAssetsResponse{
		Assets: []PluginAsset{},
	}, nil
}

func (p *YamlPlugin) CompileAssets(context.Context, CompileAssetsRequest) (*CompileAssetsResponse, error) {
	return nil, nil
}

// type yamlPluginsRepo struct { // implements IYamlPluginRepository
// 	data map[string]*YamlPlugin
// }

// func (s *yamlPluginsRepo) GetByName(name string) (*YamlPlugin, error) {
// 	if unit, ok := s.data[name]; ok {
// 		return unit, nil
// 	}
// 	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedPlugin)
// }

// func (s *yamlPluginsRepo) GetAll() []*YamlPlugin {
// 	var list []*YamlPlugin
// 	for _, unit := range s.data {
// 		list = append(list, unit)
// 	}
// 	return list
// }

// func (s *yamlPluginsRepo) GetTasks() []*YamlPlugin {
// 	var list []*YamlPlugin
// 	for _, unit := range s.data {
// 		if unit.Info.PluginType == PluginTypeTask {
// 			list = append(list, unit)
// 		}
// 	}
// 	return list
// }

// func (s *yamlPluginsRepo) GetHooks() []*YamlPlugin {
// 	var list []*YamlPlugin
// 	for _, unit := range s.data {
// 		if unit.Info.PluginType == PluginTypeHook {
// 			list = append(list, unit)
// 		}
// 	}
// 	return list
// }

// func (s *yamlPluginsRepo) Add(plugin *YamlPlugin) error {
// 	s.data[plugin.Info.Name] = plugin
// 	return nil
// }

// func (s *yamlPluginsRepo) PrintAllPlugins(logger log.Logger) {
// 	plugins := s.GetAll()
// 	logger.Info(fmt.Sprintf("\nDiscovered plugins: %d", len(plugins)))
// 	for taskIdx, plugin := range plugins {
// 		logger.Info(fmt.Sprintf("\n%d. %s", taskIdx+1, plugin.Info.Name))
// 		logger.Info(fmt.Sprintf("Description: %s", plugin.Info.Description))
// 		logger.Info(fmt.Sprintf("Image: %s", plugin.Info.Image))
// 		logger.Info(fmt.Sprintf("Type: %s", plugin.Info.PluginType))
// 		logger.Info(fmt.Sprintf("Plugin version: %s", plugin.Info.PluginVersion))
// 		logger.Info(fmt.Sprintf("Plugin mods: %v", plugin.Info.PluginMods))
// 		if plugin.Info.HookType != "" {
// 			logger.Info(fmt.Sprintf("Hook type: %s", plugin.Info.HookType))
// 		}
// 		if len(plugin.Info.DependsOn) != 0 {
// 			logger.Info(fmt.Sprintf("Depends on: %v", plugin.Info.DependsOn))
// 		}
// 	}
// }
