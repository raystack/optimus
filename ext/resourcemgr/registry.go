package resourcemgr

import (
	"errors"
	"fmt"

	"github.com/odpf/optimus/config"
)

const OptimusType = "optimus"

var Registry = &ManagerFactory{
	registry: make(map[string]NewResourceManager),
}

type NewResourceManager func(conf interface{}) (ResourceManager, error)

type ManagerFactory struct {
	registry map[string]NewResourceManager
}

func (m *ManagerFactory) Register(_type string, newResourceManager NewResourceManager) error {
	if _type == "" {
		return errors.New("type is empty")
	}
	if newResourceManager == nil {
		return errors.New("new resource manager function is nil")
	}
	if m.registry == nil {
		m.registry = make(map[string]NewResourceManager)
	}
	if m.registry[_type] != nil {
		return fmt.Errorf("type [%s] is already registered", _type)
	}
	m.registry[_type] = newResourceManager
	return nil
}

func (m *ManagerFactory) Get(_type string, conf interface{}) (ResourceManager, error) {
	if m.registry == nil {
		m.registry = make(map[string]NewResourceManager)
	}
	newResourceManager := m.registry[_type]
	if newResourceManager == nil {
		return nil, fmt.Errorf("type [%s] is not registered", _type)
	}
	return newResourceManager(conf)
}

func init() { //nolint:gochecknoinits
	err := Registry.Register(OptimusType, func(conf interface{}) (ResourceManager, error) {
		if conf == nil {
			return nil, errors.New("manager config is nil")
		}
		optimusConf, ok := conf.(config.ResourceManagerConfigOptimus)
		if !ok {
			return nil, errors.New("manager config does not follow optimus config structure")
		}
		return NewOptimusResourceManager(optimusConf)
	})
	if err != nil {
		panic(err)
	}
}
