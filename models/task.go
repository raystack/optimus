package models

import (
	"github.com/pkg/errors"
)

// Transformation needs to be implemented to register a task
type Transformation interface {
	GetName() string
	GetImage() string
	GetDescription() string

	// AskQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to GenerateConfig() parameters
	AskQuestions(UnitOptions) (map[string]interface{}, error)

	// GenerateConfig will be passed down to execution unit as env vars
	// they will be generated based on results of AskQuestions
	// if DryRun is true in UnitOptions, should not throw error for missing inputs
	GenerateConfig(inputs map[string]interface{}, opt UnitOptions) (JobSpecConfigs, error)

	// GenerateAssets will be passed down to execution unit as files
	// if DryRun is true in UnitOptions, should not throw error for missing inputs
	GenerateAssets(inputs map[string]interface{}, opt UnitOptions) (map[string]string, error)

	// GenerateDestination derive destination from config and assets
	GenerateDestination(UnitData) (string, error)

	// GetDependencies returns names of job destination on which this unit
	// is dependent on
	GenerateDependencies(UnitData) ([]string, error)
}

type UnitData struct {
	Config JobSpecConfigs
	Assets map[string]string
}

type UnitOptions struct {
	DryRun bool
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

type SupportedTaskRepo interface {
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
	if newUnit.GetName() == "" {
		return errors.New("task name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[newUnit.GetName()]; ok {
		return errors.Errorf("task name already in use %s", newUnit.GetName())
	}

	// image is a required field
	if newUnit.GetImage() == "" {
		return errors.New("task image cannot be empty")
	}

	// check if we can add the provided task
	nAssets, err := newUnit.GenerateAssets(nil, UnitOptions{DryRun: true})
	if err != nil {
		return err
	}
	for _, existingTask := range s.data {
		eAssets, _ := existingTask.GenerateAssets(nil, UnitOptions{DryRun: true})

		// config file names need to be unique in assets folder
		// so each asset name should be unique
		for ekey := range eAssets {
			for nkey := range nAssets {
				if nkey == ekey {
					return errors.Errorf("asset file name already in use %s", nkey)
				}
			}
		}
	}

	s.data[newUnit.GetName()] = newUnit
	return nil
}
