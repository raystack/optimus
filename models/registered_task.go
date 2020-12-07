package models

import (
	"errors"
	"fmt"
)

var (
	// SupportedTasks are a list of tasks that are supported as base task in a job
	SupportedTasks = &supportedTasks{
		tasks: []TaskDetails{},
	}
	ErrUnsupportedTask = errors.New("unsupported task requested")
)

type supportedTasks struct {
	tasks []TaskDetails
}

func (s *supportedTasks) GetByName(name string) (TaskDetails, error) {
	for _, t := range s.tasks {
		if t.Name == name {
			return t, nil
		}
	}
	return TaskDetails{}, ErrUnsupportedTask
}

func (s *supportedTasks) GetAll() []TaskDetails {
	return s.tasks
}

func (s *supportedTasks) Add(newTask *TaskDetails) error {
	// check if we can add the provided task
	for _, existingTask := range s.tasks {
		if newTask.Name == "" {
			return fmt.Errorf("task name cannot be empty")
		}
		if existingTask.Name == newTask.Name {
			return fmt.Errorf("task name already in use %s", newTask.Name)
		}

		// config file names need to be unique in assets folder
		for ekey := range existingTask.Asset {
			for nkey := range newTask.Asset {
				if nkey == ekey {
					return fmt.Errorf("asset file name already in use %s", nkey)
				}
			}
		}

		// all the images are required
		if newTask.Image == "" {
			return fmt.Errorf("task image cannot be empty")
		}
	}
	s.tasks = append(s.tasks, *newTask)
	return nil
}

type TaskDetails struct {
	Name   string
	Image  string
	Config map[string]string
	Asset  map[string]string
}

func init() {
	bq2bq := &TaskDetails{
		Name:  "bb_bq2bq",
		Image: "asia.gcr.io/godata-platform/bumblebee:latest",
		Config: map[string]string{
			"project":     "{{.Project}}",
			"dataset":     "{{.Dataset}}",
			"table":       "{{.Table}}",
			"load_method": "{{.LoadMethod}}",
			"sql_type":    "STANDARD",
		},
		Asset: map[string]string{
			"query.sql": `Select * from 1`,
		},
	}
	SupportedTasks.Add(bq2bq)
}
