package models

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// plugin interfaces and mods exposed to users
	PluginTypeBase = "base"

	// plugin modes are optional and implemented as needed
	ModTypeCLI                PluginMod = "cli"
	ModTypeDependencyResolver PluginMod = "dependencyresolver"

	HookTypePre  HookType = "pre"
	HookTypePost HookType = "post"
	HookTypeFail HookType = "fail"

	DestinationTypeBigquery DestinationType = "bigquery"

	DestinationURNFormat = "%s://%s"
)

var (
	// plugin types
	PluginTypeTask = PluginType(InstanceTypeTask.String())
	PluginTypeHook = PluginType(InstanceTypeHook.String())
)

type (
	PluginType string
	PluginMod  string
)

func (pm PluginMod) String() string {
	return string(pm)
}

type HookType string

func (ht HookType) String() string {
	return string(ht)
}

// BasePlugin needs to be implemented by all the plugins
type BasePlugin interface {
	PluginInfo() (*PluginInfoResponse, error)
}

type PluginInfoRequest struct{}

type PluginInfoResponse struct {
	// Name should as simple as possible with no special characters
	// should start with a character, better if all lowercase
	Name        string
	Description string
	PluginType  PluginType
	PluginMods  []PluginMod

	PluginVersion string
	APIVersion    []string

	// Image is the full path to docker container that will be
	// scheduled for execution
	Image string

	// SecretPath will be mounted inside the container as volume
	// e.g. /opt/secret/auth.json
	// here auth.json should be a key in kube secret which gets
	// translated to a file mounted in provided path
	SecretPath string

	// DependsOn returns list of hooks this should be executed after
	DependsOn []string
	// PluginType provides the place of execution, could be before the transformation
	// after the transformation, etc
	HookType HookType
}

// CommandLineMod needs to be implemented by plugins to interact with optimus CLI
type CommandLineMod interface {
	BasePlugin

	// GetQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to DefaultConfig() parameters
	GetQuestions(context.Context, GetQuestionsRequest) (*GetQuestionsResponse, error)
	ValidateQuestion(context.Context, ValidateQuestionRequest) (*ValidateQuestionResponse, error)

	// DefaultConfig will be passed down to execution unit as env vars
	// they will be generated based on results of AskQuestions
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultConfig(context.Context, DefaultConfigRequest) (*DefaultConfigResponse, error)

	// DefaultAssets will be passed down to execution unit as files
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultAssets(context.Context, DefaultAssetsRequest) (*DefaultAssetsResponse, error)

	// CompileAssets overrides default asset compilation
	CompileAssets(context.Context, CompileAssetsRequest) (*CompileAssetsResponse, error)
}

// DependencyResolverMod needs to be implemented for automatic dependency resolution of tasks
type DependencyResolverMod interface {
	BasePlugin

	// GenerateDestination derive destination from config and assets
	GenerateDestination(context.Context, GenerateDestinationRequest) (*GenerateDestinationResponse, error)

	// GenerateDependencies returns names of job destination on which this unit is dependent on
	GenerateDependencies(context.Context, GenerateDependenciesRequest) (*GenerateDependenciesResponse, error)
}

type PluginOptions struct {
	DryRun bool
}

type PluginQuestion struct {
	Name        string
	Prompt      string
	Help        string
	Default     string
	Multiselect []string

	SubQuestions []PluginSubQuestion
}

type PluginSubQuestion struct {
	// IfValue is used as an if condition to match with user input
	// if user value matches this only then ask sub questions
	IfValue   string
	Questions PluginQuestions
}

type PluginQuestions []PluginQuestion

func (q PluginQuestions) Get(name string) (PluginQuestion, bool) {
	for _, que := range q {
		if strings.EqualFold(que.Name, name) {
			return que, true
		}
	}
	return PluginQuestion{}, false
}

type PluginAnswer struct {
	Question PluginQuestion
	Value    string
}

type PluginAnswers []PluginAnswer

func (ans PluginAnswers) Get(name string) (PluginAnswer, bool) {
	for _, a := range ans {
		if strings.EqualFold(a.Question.Name, name) {
			return a, true
		}
	}
	return PluginAnswer{}, false
}

type GetQuestionsRequest struct {
	JobName string
	PluginOptions
}

type GetQuestionsResponse struct {
	Questions PluginQuestions
}

type ValidateQuestionRequest struct {
	PluginOptions

	Answer PluginAnswer
}

type ValidateQuestionResponse struct {
	Success bool
	Error   string
}

type PluginConfig struct {
	Name  string
	Value string
}

type PluginConfigs []PluginConfig

func (c PluginConfigs) Get(name string) (PluginConfig, bool) {
	for _, con := range c {
		if strings.EqualFold(con.Name, name) {
			return con, true
		}
	}
	return PluginConfig{}, false
}

func (PluginConfigs) FromJobSpec(jobSpecConfig JobSpecConfigs) PluginConfigs {
	taskPluginConfigs := PluginConfigs{}
	for _, c := range jobSpecConfig {
		taskPluginConfigs = append(taskPluginConfigs, PluginConfig(c))
	}
	return taskPluginConfigs
}

func (c PluginConfigs) ToJobSpec() JobSpecConfigs {
	jsConfigs := JobSpecConfigs{}
	for _, c := range c {
		jsConfigs = append(jsConfigs, JobSpecConfigItem(c))
	}
	return jsConfigs
}

type DefaultConfigRequest struct {
	PluginOptions

	Answers PluginAnswers
}

type DefaultConfigResponse struct {
	Config PluginConfigs
}

type PluginAsset struct {
	Name  string
	Value string
}

type PluginAssets []PluginAsset

func (c PluginAssets) Get(name string) (PluginAsset, bool) {
	for _, con := range c {
		if strings.EqualFold(con.Name, name) {
			return con, true
		}
	}
	return PluginAsset{}, false
}

func (PluginAssets) FromJobSpec(jobSpecAssets JobAssets) PluginAssets {
	taskPluginAssets := PluginAssets{}
	for _, c := range jobSpecAssets.GetAll() {
		taskPluginAssets = append(taskPluginAssets, PluginAsset(c))
	}
	return taskPluginAssets
}

func (c PluginAssets) ToJobSpec() *JobAssets {
	jsAssets := []JobSpecAsset{}
	for _, c := range c {
		jsAssets = append(jsAssets, JobSpecAsset(c))
	}
	return JobAssets{}.New(jsAssets)
}

type DefaultAssetsRequest struct {
	PluginOptions

	Answers PluginAnswers
}

type DefaultAssetsResponse struct {
	Assets PluginAssets
}

type CompileAssetsRequest struct {
	PluginOptions

	// Task configs
	Config PluginConfigs

	// Job assets
	Assets PluginAssets

	// the instance for which these assets are being compiled for
	InstanceData     []JobRunSpecData
	StartTime time.Time
	EndTime   time.Time
}

type CompileAssetsResponse struct {
	Assets PluginAssets
}

type GenerateDestinationRequest struct {
	// Task configs
	Config PluginConfigs

	// Job assets
	Assets PluginAssets

	// Deprecated: Do not use.
	Project ProjectSpec

	PluginOptions
}

type DestinationType string

func (dt DestinationType) String() string {
	return string(dt)
}

type GenerateDestinationResponse struct {
	Destination string
	Type        DestinationType
}

func (gdr GenerateDestinationResponse) URN() string {
	return fmt.Sprintf(DestinationURNFormat, gdr.Type, gdr.Destination)
}

type GenerateDependenciesRequest struct {
	// Task configs
	Config PluginConfigs

	// Job assets
	Assets PluginAssets

	// Deprecated: Do not use.
	Project ProjectSpec

	PluginOptions
}

type GenerateDependenciesResponse struct {
	Dependencies []string
}

var (
	// PluginRegistry holds all supported plugins for this run
	PluginRegistry       PluginRepository = NewPluginRepository()
	ErrUnsupportedPlugin                  = errors.New("unsupported plugin requested, make sure its correctly installed")
)

type PluginRepository interface {
	Add(BasePlugin, CommandLineMod, DependencyResolverMod) error
	GetByName(string) (*Plugin, error)
	GetAll() []*Plugin
	GetTasks() []*Plugin
	GetHooks() []*Plugin
	GetCommandLines() []CommandLineMod
	GetDependencyResolvers() []DependencyResolverMod
}

// Plugin is an extensible module implemented outside the core optimus boundaries
type Plugin struct {
	// Base is implemented by all the plugins
	Base BasePlugin

	// Mods apply multiple modifications to existing registered plugins which
	// can be used in different circumstances
	CLIMod        CommandLineMod
	DependencyMod DependencyResolverMod
}

func (p *Plugin) Info() *PluginInfoResponse {
	resp, _ := p.Base.PluginInfo()
	return resp
}

type registeredPlugins struct {
	data map[string]*Plugin
}

func (s *registeredPlugins) GetByName(name string) (*Plugin, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedPlugin)
}

func (s *registeredPlugins) GetAll() []*Plugin {
	var list []*Plugin
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *registeredPlugins) GetDependencyResolvers() []DependencyResolverMod {
	var list []DependencyResolverMod
	for _, unit := range s.data {
		if unit.DependencyMod != nil {
			list = append(list, unit.DependencyMod)
		}
	}
	return list
}

func (s *registeredPlugins) GetCommandLines() []CommandLineMod {
	var list []CommandLineMod
	for _, unit := range s.data {
		if unit.CLIMod != nil {
			list = append(list, unit.CLIMod)
		}
	}
	return list
}

func (s *registeredPlugins) GetTasks() []*Plugin {
	var list []*Plugin
	for _, unit := range s.data {
		if unit.Info().PluginType == PluginTypeTask {
			list = append(list, unit)
		}
	}
	return list
}

func (s *registeredPlugins) GetHooks() []*Plugin {
	var list []*Plugin
	for _, unit := range s.data {
		if unit.Info().PluginType == PluginTypeHook {
			list = append(list, unit)
		}
	}
	return list
}

func (s *registeredPlugins) Add(baseMod BasePlugin, cliMod CommandLineMod, drMod DependencyResolverMod) error {
	info, err := baseMod.PluginInfo()
	if err != nil {
		return err
	}
	if info.Name == "" {
		return errors.New("plugin name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[info.Name]; ok {
		return fmt.Errorf("plugin name already in use %s", info.Name)
	}

	// image is a required field
	if info.Image == "" {
		return errors.New("plugin image cannot be empty")
	}

	// version is a required field
	if info.PluginVersion == "" {
		return errors.New("plugin version cannot be empty")
	}

	switch info.PluginType {
	case PluginTypeTask:
	case PluginTypeHook:
	default:
		return ErrUnsupportedPlugin
	}

	s.data[info.Name] = &Plugin{
		Base:          baseMod,
		CLIMod:        cliMod,
		DependencyMod: drMod,
	}
	return nil
}

func NewPluginRepository() *registeredPlugins {
	return &registeredPlugins{data: map[string]*Plugin{}}
}
