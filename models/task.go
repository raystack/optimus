package models

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// TaskPlugin needs to be implemented to register a task
type TaskPlugin interface {
	GetTaskSchema(context.Context, GetTaskSchemaRequest) (GetTaskSchemaResponse, error)

	// GetTaskQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to DefaultTaskConfig() parameters
	GetTaskQuestions(context.Context, GetTaskQuestionsRequest) (GetTaskQuestionsResponse, error)
	ValidateTaskQuestion(context.Context, ValidateTaskQuestionRequest) (ValidateTaskQuestionResponse, error)

	// DefaultTaskConfig will be passed down to execution unit as env vars
	// they will be generated based on results of AskQuestions
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultTaskConfig(context.Context, DefaultTaskConfigRequest) (DefaultTaskConfigResponse, error)

	// DefaultTaskAssets will be passed down to execution unit as files
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultTaskAssets(context.Context, DefaultTaskAssetsRequest) (DefaultTaskAssetsResponse, error)

	// CompileTaskAssets overrides default asset compilation
	CompileTaskAssets(context.Context, CompileTaskAssetsRequest) (CompileTaskAssetsResponse, error)

	// GenerateTaskDestination derive destination from config and assets
	GenerateTaskDestination(context.Context, GenerateTaskDestinationRequest) (GenerateTaskDestinationResponse, error)

	// GetDependencies returns names of job destination on which this unit
	// is dependent on
	GenerateTaskDependencies(context.Context, GenerateTaskDependenciesRequest) (GenerateTaskDependenciesResponse, error)
}

type PluginOptions struct {
	DryRun bool
}

type GetTaskSchemaRequest struct{}

type GetTaskSchemaResponse struct {
	// Name should as simple as possible with no special characters
	// should start with a character, better if all lowercase
	Name        string
	Description string

	// Image is the full path to docker container that will be
	// scheduled for execution
	Image string

	// SecretPath will be mounted inside the container as volume
	// e.g. /opt/secret/auth.json
	// here auth.json should be a key in kube secret which gets
	// translated to a file mounted in provided path
	SecretPath string
}

type PluginQuestion struct {
	Name        string
	Prompt      string
	Help        string
	Default     string
	Multiselect []string

	// SubQuestionsIfValue is used as an if condition to match with user input
	// if user value matches this only then ask sub questions
	SubQuestionsIfValue string
	SubQuestions        PluginQuestions
}

type PluginQuestions []PluginQuestion

func (q PluginQuestions) Get(name string) (PluginQuestion, bool) {
	for _, que := range q {
		if strings.ToLower(que.Name) == strings.ToLower(name) {
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
		if strings.ToLower(a.Question.Name) == strings.ToLower(name) {
			return a, true
		}
	}
	return PluginAnswer{}, false
}

type GetTaskQuestionsRequest struct {
	PluginOptions
}

type GetTaskQuestionsResponse struct {
	Questions PluginQuestions
}

type ValidateTaskQuestionRequest struct {
	PluginOptions

	Answer PluginAnswer
}

type ValidateTaskQuestionResponse struct {
	Success bool
	Error   string
}

type TaskPluginConfig struct {
	Name  string
	Value string
}

type TaskPluginConfigs []TaskPluginConfig

func (c TaskPluginConfigs) Get(name string) (TaskPluginConfig, bool) {
	for _, con := range c {
		if strings.ToLower(con.Name) == strings.ToLower(name) {
			return con, true
		}
	}
	return TaskPluginConfig{}, false
}

func (c TaskPluginConfigs) FromJobSpec(jobSpecConfig JobSpecConfigs) TaskPluginConfigs {
	taskPluginConfigs := TaskPluginConfigs{}
	for _, c := range jobSpecConfig {
		taskPluginConfigs = append(taskPluginConfigs, TaskPluginConfig{

			Name:  c.Name,
			Value: c.Value,
		})
	}
	return taskPluginConfigs
}

func (c TaskPluginConfigs) ToJobSpec() JobSpecConfigs {
	jsConfigs := JobSpecConfigs{}
	for _, c := range c {
		jsConfigs = append(jsConfigs, JobSpecConfigItem{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return jsConfigs
}

type DefaultTaskConfigRequest struct {
	PluginOptions

	Answers PluginAnswers
}

type DefaultTaskConfigResponse struct {
	Config TaskPluginConfigs
}

type TaskPluginAsset struct {
	Name  string
	Value string
}

type TaskPluginAssets []TaskPluginAsset

func (c TaskPluginAssets) Get(name string) (TaskPluginAsset, bool) {
	for _, con := range c {
		if strings.ToLower(con.Name) == strings.ToLower(name) {
			return con, true
		}
	}
	return TaskPluginAsset{}, false
}

func (c TaskPluginAssets) FromJobSpec(jobSpecAssets JobAssets) TaskPluginAssets {
	taskPluginAssets := TaskPluginAssets{}
	for _, c := range jobSpecAssets.GetAll() {
		taskPluginAssets = append(taskPluginAssets, TaskPluginAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return taskPluginAssets
}

func (c TaskPluginAssets) ToJobSpec() *JobAssets {
	jsAssets := []JobSpecAsset{}
	for _, c := range c {
		jsAssets = append(jsAssets, JobSpecAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return JobAssets{}.New(jsAssets)
}

type DefaultTaskAssetsRequest struct {
	PluginOptions

	Answers PluginAnswers
}

type DefaultTaskAssetsResponse struct {
	Assets TaskPluginAssets
}

type CompileTaskAssetsRequest struct {
	PluginOptions

	// Task configs
	Config     TaskPluginConfigs
	TaskWindow JobSpecTaskWindow

	// Job assets
	Assets TaskPluginAssets

	// the instance for which these assets are being compiled for
	InstanceData     []InstanceSpecData
	InstanceSchedule time.Time
}

type CompileTaskAssetsResponse struct {
	Assets TaskPluginAssets
}

type GenerateTaskDestinationRequest struct {
	// Task configs
	Config TaskPluginConfigs

	// Job assets
	Assets TaskPluginAssets

	// Job project
	Project ProjectSpec

	PluginOptions
}

type GenerateTaskDestinationResponse struct {
	Destination string
}

type GenerateTaskDependenciesRequest struct {
	// Task configs
	Config TaskPluginConfigs

	// Job assets
	Assets TaskPluginAssets

	// Job project
	Project ProjectSpec

	PluginOptions
}

type GenerateTaskDependenciesResponse struct {
	Dependencies []string
}

var (
	// TaskRegistry is a list of tasks that are supported as base task in a job
	TaskRegistry       TaskPluginRepository = NewTaskPluginRepository()
	ErrUnsupportedTask                      = errors.New("unsupported task requested")
)

type TaskPluginRepository interface {
	GetByName(string) (TaskPlugin, error)
	GetAll() []TaskPlugin
	Add(TaskPlugin) error
}

type supportedTasks struct {
	data map[string]TaskPlugin
}

func (s *supportedTasks) GetByName(name string) (TaskPlugin, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, errors.Wrap(ErrUnsupportedTask, name)
}

func (s *supportedTasks) GetAll() []TaskPlugin {
	var list []TaskPlugin
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *supportedTasks) Add(newUnit TaskPlugin) error {
	schema, err := newUnit.GetTaskSchema(context.Background(), GetTaskSchemaRequest{})
	if err != nil {
		return err
	}
	if schema.Name == "" {
		return errors.New("task name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[schema.Name]; ok {
		return errors.Errorf("task name already in use %s", schema.Name)
	}

	// image is a required field
	if schema.Image == "" {
		return errors.New("task image cannot be empty")
	}

	// check if we can add the provided task
	nAssets, err := newUnit.DefaultTaskAssets(context.Background(), DefaultTaskAssetsRequest{
		PluginOptions: PluginOptions{
			DryRun: true,
		},
	})
	if err != nil {
		return err
	}
	for _, existingTask := range s.data {
		response, _ := existingTask.DefaultTaskAssets(context.Background(), DefaultTaskAssetsRequest{
			PluginOptions: PluginOptions{
				DryRun: true,
			},
		})

		// config file names need to be unique in assets folder
		// so each asset name should be unique
		for _, ekey := range response.Assets {
			for _, nkey := range nAssets.Assets {
				if nkey.Name == ekey.Name {
					return errors.Errorf("asset file name already in use %s", nkey.Name)
				}
			}
		}
	}

	s.data[schema.Name] = newUnit
	return nil
}

func NewTaskPluginRepository() *supportedTasks {
	return &supportedTasks{data: map[string]TaskPlugin{}}
}
