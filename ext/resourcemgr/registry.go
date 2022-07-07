package resourcemgr

import "github.com/odpf/optimus/config"

const OptimusType = "optimus"

var Registry = &ManagerRegistry{}

type newResourceManager func(conf interface{}) (ResourceManager, error)

type ManagerRegistry struct {
	registry map[string]newResourceManager
}

func (m ManagerRegistry) Get(_type string, conf interface{}) (ResourceManager, error) {
	newResourceManager, ok := m.registry[_type]
	if !ok {
		return nil, nil
	}
	return newResourceManager(conf)
}

func init() {
	Registry.registry[OptimusType] = func(conf interface{}) (ResourceManager, error) {
		optimusConfig, ok := conf.(config.ResourceManagerConfigOptimus)
		if !ok {
			return nil, nil
		}
		return NewOptimusResourceManager(optimusConfig)
	}
}
