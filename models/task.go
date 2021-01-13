package models

import (
	"errors"
	"fmt"

	"github.com/AlecAivazis/survey/v2"
)

// ExecutionUnit needs to be implemented to register a task
type ExecutionUnit interface {
	GetName() string
	GetImage() string
	GetAssets() map[string]string
	GetDescription() string

	//GetQuestions list down all the cli inputs required to generate spec files
	//name used for question will be directly mapped to GetConfig() parameters
	GetQuestions() []*survey.Question

	//GetConfig will be passed down to execution unit as env vars
	//they can be templatized by enclosing question `name` parameter inside doube braces preceded by .
	//for example
	//"project": "{{.Project}}"
	//where `Project` is a question asked by user in GetQuestions
	GetConfig() map[string]string
}

var (
	// SupportedTasks are a list of tasks that are supported as base task in a job
	SupportedTasks = &supportedTasks{
		tasks: map[string]ExecutionUnit{},
	}
	ErrUnsupportedTask = errors.New("unsupported task requested")
)

type supportedTasks struct {
	tasks map[string]ExecutionUnit
}

func (s *supportedTasks) GetByName(name string) (ExecutionUnit, error) {
	if task, ok := s.tasks[name]; ok {
		return task, nil
	}
	return nil, ErrUnsupportedTask
}

func (s *supportedTasks) GetAll() []ExecutionUnit {
	list := []ExecutionUnit{}
	for _, task := range s.tasks {
		list = append(list, task)
	}
	return list
}

func (s *supportedTasks) Add(newTask ExecutionUnit) error {
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
