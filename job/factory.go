package job

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

var Registry = &ExternalDependencyGetterFactory{}

type ExternalDependencyGetter interface {
	GetExternalDependency(context.Context, models.JobSpecFilter) (models.ExternalDependency, error)
}

type NewExternalDependencyGetter func(config.ResourceManager) (ExternalDependencyGetter, error)

type ExternalDependencyGetterFactory struct {
	registry map[string]NewExternalDependencyGetter
}

func (m *ExternalDependencyGetterFactory) Register(_type string, newExternalDependencyGetter NewExternalDependencyGetter) error {
	if _type == "" {
		return errors.New("type is empty")
	}
	if newExternalDependencyGetter == nil {
		return errors.New("new external dependency getter function is nil")
	}
	if m.registry == nil {
		m.registry = make(map[string]NewExternalDependencyGetter)
	}
	if m.registry[_type] != nil {
		return fmt.Errorf("type [%s] is already registered", _type)
	}
	m.registry[_type] = newExternalDependencyGetter
	return nil
}

func (m *ExternalDependencyGetterFactory) Get(conf config.ResourceManager) (ExternalDependencyGetter, error) {
	if m.registry == nil {
		m.registry = make(map[string]NewExternalDependencyGetter)
	}
	newResourceManager := m.registry[conf.Type]
	if newResourceManager == nil {
		return nil, fmt.Errorf("type [%s] is not registered", conf.Type)
	}
	return newResourceManager(conf)
}
