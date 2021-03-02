package models

import (
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pkg/errors"
)

const (
	HookTypePre  HookType = "pre"
	HookTypePost HookType = "post"
)

type HookType string

type HookUnit interface {
	GetName() string
	GetImage() string
	GetDescription() string

	// GetQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to GetConfig() parameters
	GetQuestions() []*survey.Question

	// GetConfig will be passed down to hook unit as env vars
	// they can be templatized by enclosing question `name` parameter inside double braces preceded by .
	// for example, "project": "{{.Project}}" where `Project` is a question asked by user in GetQuestions.
	//
	// you can also save templates within the job spec eg, "BROKERS": `{{ "{{.transporterKafkaBroker}}" }}` will
	// store "BROKERS": '{{.transporterKafkaBroker}}' inside the job spec; which gets compiled by taking values
	// from project config or runtime variables provided part of a instance. i.e.
	// DSTART, DEND, EXECUTION_TIME
	GetConfig(jobUnitData UnitData) (map[string]string, error)

	// GetDependsOn returns list of hooks this should be executed after
	GetDependsOn() []string

	// GetType provides the place of execution, could be before the transformation
	// after the transformation, etc
	GetType() HookType
}

var (
	// HookRegistry are a list of hooks that are supported in a job
	HookRegistry = &supportedHooks{
		data: map[string]HookUnit{},
	}
	ErrUnsupportedHook = errors.New("unsupported hook requested")
)

type SupportedHookRepo interface {
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
	if newUnit.GetName() == "" {
		return fmt.Errorf("hook name cannot be empty")
	}

	// check if name is already used
	if _, ok := s.data[newUnit.GetName()]; ok {
		return fmt.Errorf("hook name already in use %s", newUnit.GetName())
	}

	// image is a required field
	if newUnit.GetImage() == "" {
		return fmt.Errorf("hook image cannot be empty")
	}

	s.data[newUnit.GetName()] = newUnit
	return nil
}
