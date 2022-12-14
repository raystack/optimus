package models

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
)

const (
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

type PluginInfoResponse struct {
	// Name should as simple as possible with no special characters
	// should start with a character, better if all lowercase
	Name        string
	Description string
	PluginType  PluginType  `yaml:",omitempty"`
	PluginMods  []PluginMod `yaml:",omitempty"`

	PluginVersion string   `yaml:",omitempty"`
	APIVersion    []string `yaml:",omitempty"`

	// Image is the full path to docker container that will be
	// scheduled for execution
	Image string

	// SecretPath will be mounted inside the container as volume
	// e.g. /opt/secret/auth.json
	// here auth.json should be a key in kube secret which gets
	// translated to a file mounted in provided path
	SecretPath string `yaml:",omitempty"`

	// DependsOn returns list of hooks this should be executed after
	DependsOn []string `yaml:",omitempty"`
	// PluginType provides the place of execution, could be before the transformation
	// after the transformation, etc
	HookType HookType `yaml:",omitempty"`
}

// CommandLineMod needs to be implemented by plugins to interact with optimus CLI
type CommandLineMod interface {
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
}

// DependencyResolverMod needs to be implemented for automatic dependency resolution of tasks
type DependencyResolverMod interface {
	// GetName returns name of the plugin
	GetName(context.Context) (string, error)

	// GenerateDestination derive destination from config and assets
	GenerateDestination(context.Context, GenerateDestinationRequest) (*GenerateDestinationResponse, error)

	// GenerateDependencies returns names of job destination on which this unit is dependent on
	GenerateDependencies(context.Context, GenerateDependenciesRequest) (*GenerateDependenciesResponse, error)

	// CompileAssets overrides default asset compilation
	CompileAssets(context.Context, CompileAssetsRequest) (*CompileAssetsResponse, error)
}

type YamlMod interface {
	PluginInfo() *PluginInfoResponse
	CommandLineMod
}

type PluginOptions struct {
	DryRun bool
}

// USED in models.PluginQuestion Validations
type vFactory struct{}

func (*vFactory) NewFromRegex(re, message string) survey.Validator {
	var regex = regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val := v.(string)
		if !regex.Match([]byte(val)) {
			return fmt.Errorf("%s", message)
		}
		return nil
	}
}

var ValidatorFactory = new(vFactory)

type PluginQuestion struct {
	Name        string   `yaml:",omitempty"`
	Prompt      string   `yaml:",omitempty"`
	Help        string   `yaml:",omitempty"`
	Default     string   `yaml:",omitempty"`
	Multiselect []string `yaml:",omitempty"`

	SubQuestions []PluginSubQuestion `yaml:",omitempty"`

	Regexp          string `yaml:",omitempty"`
	ValidationError string `yaml:",omitempty"`
	MinLength       int    `yaml:",omitempty"`
	MaxLength       int    `yaml:",omitempty"`
	Required        bool   `yaml:",omitempty"`
}

func (q *PluginQuestion) IsValid(value string) error {
	if q.Required {
		return survey.Required(value)
	}
	var validators []survey.Validator
	if q.Regexp != "" {
		validators = append(validators, ValidatorFactory.NewFromRegex(q.Regexp, q.ValidationError))
	}
	if q.MinLength != 0 {
		validators = append(validators, survey.MinLength(q.MinLength))
	}
	if q.MaxLength != 0 {
		validators = append(validators, survey.MaxLength(q.MaxLength))
	}
	return survey.ComposeValidators(validators...)(value)
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
	Questions PluginQuestions `yaml:",omitempty"`
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

func (PluginConfigs) FromMap(configMap map[string]string) PluginConfigs {
	taskPluginConfigs := PluginConfigs{}
	for key, value := range configMap {
		taskPluginConfigs = append(taskPluginConfigs, PluginConfig{
			Name:  key,
			Value: value,
		})
	}
	return taskPluginConfigs
}

type DefaultConfigRequest struct {
	PluginOptions

	Answers PluginAnswers
}

type DefaultConfigResponse struct {
	Config PluginConfigs `yaml:"defaultconfig,omitempty"`
}

type PluginAsset struct {
	Name  string
	Value string
}

type PluginAssets []PluginAsset

func (PluginAssets) FromMap(assetsMap map[string]string) PluginAssets {
	taskPluginAssets := PluginAssets{}
	for key, value := range assetsMap {
		taskPluginAssets = append(taskPluginAssets, PluginAsset{
			Name:  key,
			Value: value,
		})
	}
	return taskPluginAssets
}

func (c PluginAssets) ToMap() map[string]string {
	mapping := map[string]string{}
	for _, asset := range c {
		mapping[asset.Name] = asset.Value
	}
	return mapping
}

type DefaultAssetsRequest struct {
	PluginOptions

	Answers PluginAnswers
}

type DefaultAssetsResponse struct {
	Assets PluginAssets `yaml:"defaultassets,omitempty"`
}

type CompileAssetsRequest struct {
	PluginOptions

	// Task configs
	Config PluginConfigs

	// Job assets
	Assets PluginAssets

	// the instance for which these assets are being compiled for
	InstanceData []JobRunSpecData
	StartTime    time.Time
	EndTime      time.Time
}

type CompileAssetsResponse struct {
	Assets PluginAssets
}

type GenerateDestinationRequest struct {
	// Task configs
	Config PluginConfigs

	// Job assets
	Assets PluginAssets

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

	PluginOptions
}

type GenerateDependenciesResponse struct {
	Dependencies []string
}

var (
	ErrUnsupportedPlugin = errors.New("unsupported plugin requested, make sure its correctly installed")
)

type PluginRepository interface {
	AddYaml(YamlMod) error                 // yaml plugin
	AddBinary(DependencyResolverMod) error // binary plugin
	GetByName(string) (*Plugin, error)
	GetAll() []*Plugin
	GetTasks() []*Plugin
	GetHooks() []*Plugin
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

func (p *Plugin) Info() *PluginInfoResponse {
	if p.YamlMod != nil {
		return p.YamlMod.PluginInfo()
	}
	return nil
}

type RegisteredPlugins struct {
	data       map[string]*Plugin
	sortedKeys []string
}

func (s *RegisteredPlugins) lazySortPluginKeys() {
	// already sorted
	if len(s.data) == 0 || len(s.sortedKeys) > 0 {
		return
	}

	for k := range s.data {
		s.sortedKeys = append(s.sortedKeys, k)
	}
	sort.Strings(s.sortedKeys)
}

func (s *RegisteredPlugins) GetByName(name string) (*Plugin, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, fmt.Errorf("%s: %w", name, ErrUnsupportedPlugin)
}

func (s *RegisteredPlugins) GetAll() []*Plugin {
	var list []*Plugin
	s.lazySortPluginKeys() // sorts keys if not sorted
	for _, pluginName := range s.sortedKeys {
		list = append(list, s.data[pluginName])
	}
	return list
}

func (s *RegisteredPlugins) GetTasks() []*Plugin {
	var list []*Plugin
	s.lazySortPluginKeys() // sorts keys if not sorted
	for _, pluginName := range s.sortedKeys {
		unit := s.data[pluginName]
		if unit.Info().PluginType == PluginTypeTask {
			list = append(list, unit)
		}
	}
	return list
}

func (s *RegisteredPlugins) GetHooks() []*Plugin {
	var list []*Plugin
	s.lazySortPluginKeys()
	for _, pluginName := range s.sortedKeys {
		unit := s.data[pluginName]
		if unit.Info().PluginType == PluginTypeHook {
			list = append(list, unit)
		}
	}
	return list
}

// for addin yaml plugins
func (s *RegisteredPlugins) AddYaml(yamlMod YamlMod) error {
	info := yamlMod.PluginInfo()
	if err := validateYamlPluginInfo(info); err != nil {
		return err
	}

	if _, ok := s.data[info.Name]; ok {
		// duplicated yaml plugin
		return fmt.Errorf("plugin name already in use %s", info.Name)
	}

	s.data[info.Name] = &Plugin{YamlMod: yamlMod}
	return nil
}

// for addin binary plugins
func (s *RegisteredPlugins) AddBinary(drMod DependencyResolverMod) error {
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

func validateYamlPluginInfo(info *PluginInfoResponse) error {
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

	switch info.PluginType {
	case PluginTypeTask:
	case PluginTypeHook:
	default:
		return ErrUnsupportedPlugin
	}

	return nil
}

func NewPluginRepository() *RegisteredPlugins {
	return &RegisteredPlugins{data: map[string]*Plugin{}}
}