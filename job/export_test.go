package job

import (
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/store"
)

func NewTestExternalDependencyResolver(
	optimusResourceManagers []resourcemgr.ResourceManager,
	unknownJobDependencyRepository store.UnknownJobDependencyRepository,
) ExternalDependencyResolver {
	return &externalDependencyResolver{
		unknownJobDependencyRepository: unknownJobDependencyRepository,
		optimusResourceManagers:        optimusResourceManagers,
	}
}
