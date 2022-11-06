package resolver

import (
	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/core/job"

	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job/dto"
)

type ExtUpstreamResolver struct {
	optimusResourceManagers []ResourceManager
}

// ResourceManager is repository for external job spec
type ResourceManager interface {
	GetOptimusUpstreams(context.Context, *dto.RawUpstream) ([]*job.Upstream, error)
}

// NewExternalUpstreamResolver creates a new instance of externalUpstreamResolver
func NewExternalUpstreamResolver(resourceManagers []ResourceManager) *ExtUpstreamResolver {
	return &ExtUpstreamResolver{
		optimusResourceManagers: resourceManagers,
	}
}

func (e *ExtUpstreamResolver) FetchExternalUpstreams(ctx context.Context, unresolvedUpstreams []*dto.RawUpstream) ([]*job.Upstream, []*dto.RawUpstream, error) {
	var unknownUpstreams []*dto.RawUpstream
	var externalUpstreams []*job.Upstream
	var allErrors error
	for _, toBeResolvedUpstream := range unresolvedUpstreams {
		optimusUpstreams, err := e.fetchOptimusUpstreams(ctx, toBeResolvedUpstream)
		if err != nil {
			unknownUpstreams = append(unknownUpstreams, toBeResolvedUpstream)
			allErrors = multierror.Append(allErrors, err)
			continue
		}
		externalUpstreams = append(externalUpstreams, optimusUpstreams...)
	}
	return externalUpstreams, unknownUpstreams, allErrors
}

func (e *ExtUpstreamResolver) fetchOptimusUpstreams(ctx context.Context, unresolvedUpstream *dto.RawUpstream) ([]*job.Upstream, error) {
	var upstreams []*job.Upstream
	var allErrors error
	for _, manager := range e.optimusResourceManagers {
		deps, err := manager.GetOptimusUpstreams(ctx, unresolvedUpstream)
		if err != nil {
			allErrors = multierror.Append(allErrors, err)
			continue
		}
		upstreams = append(upstreams, deps...)
	}
	return upstreams, allErrors
}
