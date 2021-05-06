package models

import (
	"fmt"

	"github.com/pkg/errors"
)

const (
	HookTypePre  HookType = "pre"
	HookTypePost HookType = "post"
)

type HookType string

func (ht HookType) String() string {
	return string(ht)
}

type HookUnit interface {
	Name() string
	Image() string
	Description() string

	// DependsOn returns list of hooks this should be executed after
	DependsOn() []string

	// GetType provides the place of execution, could be before the transformation
	// after the transformation, etc
	Type() HookType

	// GetQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to GenerateConfig() parameters
	AskQuestions(request AskQuestionRequest) (AskQuestionResponse, error)

	// GenerateConfig will be passed down to hook unit as env vars
	// hookInputs - answers for the questions as inputs from user
	// unitData - parent task configs
	//
	// using templates within the hook config, eg "BROKERS": `{{ "{{.transporterKafkaBroker}}" }}`, will
	// store "BROKERS": '{{.transporterKafkaBroker}}' inside the job spec; which gets compiled by taking values
	// from project config or runtime variables provided part of a instance. i.e.
	// DSTART, DEND, EXECUTION_TIME
	GenerateConfig(GenerateConfigWithTaskRequest) (DefaultConfigResponse, error)
}

type GenerateConfigWithTaskRequest struct {
	// TaskConfig of the parent on which this task belongs to
	TaskConfig JobSpecConfigs

	DefaultConfigRequest
}

var (
	// HookRegistry are a list of hooks that are supported in a job
	HookRegistry = &supportedHooks{
		data: map[string]HookUnit{},
	}
	ErrUnsupportedHook = errors.New("unsupported hook requested")
)

type HookRepo interface {
	GetByName(string) (HookUnit, error)
	GetAll() []HookUnit
	Add(HookUnit) error
}

type supportedHooks struct {
	data map[string]HookUnit
}

func (s *supportedHooks) GetByName(name string) (HookUnit, error) {
	if unit, ok := s.data[name]; ok {
		return unit, nil
	}
	return nil, errors.Wrap(ErrUnsupportedHook, name)
}

func (s *supportedHooks) GetAll() []HookUnit {
	list := []HookUnit{}
	for _, unit := range s.data {
		list = append(list, unit)
	}
	return list
}

func (s *supportedHooks) Add(newUnit HookUnit) error {
	if newUnit.Name() == "" {
		return fmt.Errorf("hook name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[newUnit.Name()]; ok {
		return fmt.Errorf("hook name already in use %s", newUnit.Name())
	}

	// image is a required field
	if newUnit.Image() == "" {
		return fmt.Errorf("hook image cannot be empty")
	}

	s.data[newUnit.Name()] = newUnit
	return nil
}
