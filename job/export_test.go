package job

import (
	"github.com/odpf/optimus/ext/resourcemgr"
)

func NewTestExternalDependencyResolver(
	optimusResourceManagers []resourcemgr.ResourceManager,
) ExternalDependencyResolver {
	return &externalDependencyResolver{
		optimusResourceManagers: optimusResourceManagers,
	}
}
