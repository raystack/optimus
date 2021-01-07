package models

import (
	"errors"
	"fmt"

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
	// SupportedTasks are a list of tasks that are supported as base task in a job
	SupportedTasks = &supportedTasks{
		tasks: map[string]ExecUnit{},
	}
	ErrUnsupportedTask = errors.New("unsupported task requested")
)

type SupportedTaskRepo interface {
	GetByName(string) (ExecUnit, error)
	GetAll() []ExecUnit
	Add(ExecUnit) error
}

type supportedTasks struct {
	tasks map[string]ExecUnit
}

func (s *supportedTasks) GetByName(name string) (ExecUnit, error) {
	if task, ok := s.tasks[name]; ok {
		return task, nil
	}
	return nil, ErrUnsupportedTask
}

func (s *supportedTasks) GetAll() []ExecUnit {
	list := []ExecUnit{}
	for _, task := range s.tasks {
		list = append(list, task)
	}
	return list
}

func (s *supportedTasks) Add(newTask ExecUnit) error {
	if newTask.GetName() == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.tasks[newTask.GetName()]; ok {
		return fmt.Errorf("task name already in use %s", newTask.GetName())
	}

	// image is a required field
	if newTask.GetImage() == "" {
		return fmt.Errorf("task image cannot be empty")
	}

	// check if we can add the provided task
	for _, existingTask := range s.tasks {
		// config file names need to be unique in assets folder
		// so each asset name should be unique
		for ekey := range existingTask.GetAssets() {
			for nkey := range newTask.GetAssets() {
				if nkey == ekey {
					return fmt.Errorf("asset file name already in use %s", nkey)
				}
			}
		}
	}

	s.tasks[newTask.GetName()] = newTask
	return nil
}
