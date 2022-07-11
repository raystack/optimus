package job

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/store"
)

func NewTestOptimusDependencyGetter(
	resourceManagerName string,
	resourceManagerConfig config.ResourceManagerConfigOptimus,
	resourceManager resourcemgr.ResourceManager,
) OptimusDependencyGetter {
	return &optimusDependencyGetter{
		resourceManagerName:   resourceManagerName,
		resourceManagerConfig: resourceManagerConfig,
		resourceManager:       resourceManager,
	}
}

func NewTestExternalDependencyResolver(
	optimusDependencyGetters []OptimusDependencyGetter,
	unknownJobDependencyRepository store.UnknownJobDependencyRepository,
) ExternalDependencyResolver {
	return &externalDependencyResolver{
		unknownJobDependencyRepository: unknownJobDependencyRepository,
		optimusDependencyGetters:       optimusDependencyGetters,
	}
}
