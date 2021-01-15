package models

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/AlecAivazis/survey/v2"
)

// ExecUnit needs to be implemented to register a task
type ExecUnit interface {
	GetName() string
	GetImage() string
	GetAssets() map[string]string
	GetDescription() string

	// GetQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to GetConfig() parameters
	GetQuestions() []*survey.Question

	// GetConfig will be passed down to execution unit as env vars
	// they can be templatized by enclosing question `name` parameter inside doube braces preceded by .
	// for example
	// "project": "{{.Project}}"
	// where `Project` is a question asked by user in GetQuestions
	GetConfig() map[string]string

	// GenerateDestination derive destination from config and assets
	GenerateDestination(UnitData) (string, error)

	// GetDependencies returns names of job destiantion on which this unit
	// is dependent on
	GenerateDependencies(UnitData) ([]string, error)
}

type UnitData struct {
	Config map[string]string
	Assets map[string]string
}

var (
	// TaskRegistry are a list of tasks that are supported as base task in a job
	TaskRegistry = &supportedTasks{
		data: map[string]ExecUnit{},
	}
	ErrUnsupportedTask = errors.New("unsupported task requested")
)

type supportedTasks struct {
	data map[string]ExecUnit
}

type SupportedTaskRepo interface {
	GetByName(string) (ExecUnit, error)
	GetAll() []ExecUnit
	Add(ExecUnit) error
}

func (s *supportedTasks) GetByName(name string) (ExecUnit, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, errors.Wrap(ErrUnsupportedTask, name)
}

func (s *supportedTasks) GetAll() []ExecUnit {
	list := []ExecUnit{}
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *supportedTasks) Add(newUnit ExecUnit) error {
	if newUnit.GetName() == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[newUnit.GetName()]; ok {
		return fmt.Errorf("task name already in use %s", newUnit.GetName())
	}

	// image is a required field
	if newUnit.GetImage() == "" {
		return fmt.Errorf("task image cannot be empty")
	}

	// check if we can add the provided task
	for _, existingTask := range s.data {
		// config file names need to be unique in assets folder
		// so each asset name should be unique
		for ekey := range existingTask.GetAssets() {
			for nkey := range newUnit.GetAssets() {
				if nkey == ekey {
					return fmt.Errorf("asset file name already in use %s", nkey)
				}
			}
		}
	}

	s.data[newUnit.GetName()] = newUnit
	return nil
}
