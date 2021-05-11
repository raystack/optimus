package models

import (
	"time"

	"github.com/pkg/errors"
)

// Transformation needs to be implemented to register a task
type Transformation interface {
	Name() string
	Image() string
	Description() string

	// AskQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to GenerateConfig() parameters
	AskQuestions(AskQuestionRequest) (AskQuestionResponse, error)

	// DefaultConfig will be passed down to execution unit as env vars
	// they will be generated based on results of AskQuestions
	// if DryRun is true in UnitOptions, should not throw error for missing inputs
	DefaultConfig(DefaultConfigRequest) (DefaultConfigResponse, error)

	// DefaultAssets will be passed down to execution unit as files
	// if DryRun is true in UnitOptions, should not throw error for missing inputs
	DefaultAssets(DefaultAssetsRequest) (DefaultAssetsResponse, error)

	CompileAssets(CompileAssetsRequest) (CompileAssetsResponse, error)

	// GenerateDestination derive destination from config and assets
	GenerateDestination(GenerateDestinationRequest) (GenerateDestinationResponse, error)

	// GetDependencies returns names of job destination on which this unit
	// is dependent on
	GenerateDependencies(GenerateDependenciesRequest) (GenerateDependenciesResponse, error)
}

type UnitOptions struct {
	DryRun bool
}

type AskQuestionRequest struct {
	UnitOptions
}

type AskQuestionResponse struct {
	Answers map[string]interface{}
}

type DefaultConfigRequest struct {
	Inputs map[string]interface{}
	UnitOptions
}

type DefaultConfigResponse struct {
	Config JobSpecConfigs
}

type DefaultAssetsRequest struct {
	Inputs map[string]interface{}
	UnitOptions
}

type DefaultAssetsResponse struct {
	Assets map[string]string
}

type CompileAssetsRequest struct {
	// Window
	TaskWindow JobSpecTaskWindow

	// Task configs
	Config JobSpecConfigs

	// Job assets
	Assets map[string]string

	InstanceSchedule time.Time
	InstanceData     []InstanceSpecData

	UnitOptions
}

type CompileAssetsResponse struct {
	Assets map[string]string
}

type GenerateDestinationRequest struct {
	// Task configs
	Config JobSpecConfigs

	// Job assets
	Assets map[string]string

	// Job project
	Project ProjectSpec

	UnitOptions
}

type GenerateDestinationResponse struct {
	Destination string
}

type GenerateDependenciesRequest struct {
	// Task configs
	Config JobSpecConfigs

	// Job assets
	Assets map[string]string

	// Job project
	Project ProjectSpec

	UnitOptions
}

type GenerateDependenciesResponse struct {
	Dependencies []string
}

var (
	// TaskRegistry is a list of tasks that are supported as base task in a job
	TaskRegistry = &supportedTasks{
		data: map[string]Transformation{},
	}
	ErrUnsupportedTask = errors.New("unsupported task requested")
)

type supportedTasks struct {
	data map[string]Transformation
}

type TransformationRepo interface {
	GetByName(string) (Transformation, error)
	GetAll() []Transformation
	Add(Transformation) error
}

func (s *supportedTasks) GetByName(name string) (Transformation, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, errors.Wrap(ErrUnsupportedTask, name)
}

func (s *supportedTasks) GetAll() []Transformation {
	var list []Transformation
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *supportedTasks) Add(newUnit Transformation) error {
	if newUnit.Name() == "" {
		return errors.New("task name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[newUnit.Name()]; ok {
		return errors.Errorf("task name already in use %s", newUnit.Name())
	}

	// image is a required field
	if newUnit.Image() == "" {
		return errors.New("task image cannot be empty")
	}

	// check if we can add the provided task
	nAssets, err := newUnit.DefaultAssets(DefaultAssetsRequest{
		UnitOptions: UnitOptions{
			DryRun: true,
		},
	})
	if err != nil {
		return err
	}
	for _, existingTask := range s.data {
		response, _ := existingTask.DefaultAssets(DefaultAssetsRequest{
			UnitOptions: UnitOptions{
				DryRun: true,
			},
		})

		// config file names need to be unique in assets folder
		// so each asset name should be unique
		for ekey := range response.Assets {
			for nkey := range nAssets.Assets {
				if nkey == ekey {
					return errors.Errorf("asset file name already in use %s", nkey)
				}
			}
		}
	}

	s.data[newUnit.Name()] = newUnit
	return nil
}
