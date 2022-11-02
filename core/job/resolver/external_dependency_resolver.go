package resolver

import (
	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/core/job"

	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job/dto"
)

type ExtDependencyResolver struct {
	optimusResourceManagers []ResourceManager
}

// ResourceManager is repository for external job spec
type ResourceManager interface {
	GetOptimusDependencies(context.Context, *dto.RawDependency) ([]*job.Dependency, error)
}

// NewExternalDependencyResolver creates a new instance of externalDependencyResolver
func NewExternalDependencyResolver(resourceManagers []ResourceManager) *ExtDependencyResolver {
	return &ExtDependencyResolver{
		optimusResourceManagers: resourceManagers,
	}
}

func (e *ExtDependencyResolver) FetchExternalDependencies(ctx context.Context, unresolvedDependencies []*dto.RawDependency) ([]*job.Dependency, []*dto.RawDependency, error) {
	var unknownDependencies []*dto.RawDependency
	var externalDependencies []*job.Dependency
	var allErrors error
	for _, toBeResolvedDependency := range unresolvedDependencies {
		optimusDependencies, err := e.fetchOptimusDependencies(ctx, toBeResolvedDependency)
		if err != nil {
			unknownDependencies = append(unknownDependencies, toBeResolvedDependency)
			allErrors = multierror.Append(allErrors, err)
			continue
		}
		externalDependencies = append(externalDependencies, optimusDependencies...)
	}
	return externalDependencies, unknownDependencies, allErrors
}

func (e *ExtDependencyResolver) fetchOptimusDependencies(ctx context.Context, unresolvedDependency *dto.RawDependency) ([]*job.Dependency, error) {
	var dependencies []*job.Dependency
	var allErrors error
	for _, manager := range e.optimusResourceManagers {
		deps, err := manager.GetOptimusDependencies(ctx, unresolvedDependency)
		if err != nil {
			allErrors = multierror.Append(allErrors, err)
			continue
		}
		dependencies = append(dependencies, deps...)
	}
	return dependencies, allErrors
}
