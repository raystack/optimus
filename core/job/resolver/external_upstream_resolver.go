package resolver

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/resourcemanager"
	"github.com/odpf/optimus/internal/errors"
)

type extUpstreamResolver struct {
	optimusResourceManagers []resourcemanager.ResourceManager
}

// NewExternalUpstreamResolver creates a new instance of externalUpstreamResolver
func NewExternalUpstreamResolver(resourceManagerConfigs []config.ResourceManager) (*extUpstreamResolver, error) {
	var optimusResourceManagers []resourcemanager.ResourceManager
	for _, conf := range resourceManagerConfigs {
		switch conf.Type {
		case "optimus":
			getter, err := resourcemanager.NewOptimusResourceManager(conf)
			if err != nil {
				return nil, err
			}
			optimusResourceManagers = append(optimusResourceManagers, getter)
		default:
			return nil, fmt.Errorf("resource manager %s is not recognized", conf.Type)
		}
	}
	return &extUpstreamResolver{
		optimusResourceManagers: optimusResourceManagers,
	}, nil
}

type ResourceManager interface {
	GetOptimusUpstreams(ctx context.Context, unresolvedDependency *dto.RawUpstream) ([]*job.Upstream, error)
}

func (e *extUpstreamResolver) Resolve(ctx context.Context, upstreamsToResolve []*dto.RawUpstream) ([]*job.Upstream, []*job.Upstream, error) {
	externalUpstreams, unresolvedUpstreams, err := e.fetchExternalUpstreams(ctx, upstreamsToResolve)

	var unknownUpstreams []*job.Upstream
	for _, upstream := range unresolvedUpstreams {
		// allow empty upstreamName and upstreamProjectName
		upstreamName, _ := job.NameFrom(upstream.JobName)
		upstreamProjectName, _ := tenant.ProjectNameFrom(upstream.ProjectName)
		upstreamResourceURN := job.ResourceURN(upstream.ResourceURN)
		unknownUpstreams = append(unknownUpstreams, job.NewUpstreamUnresolved(upstreamName, upstreamResourceURN, upstreamProjectName))
	}

	return externalUpstreams, unknownUpstreams, err
}

func (e *extUpstreamResolver) fetchExternalUpstreams(ctx context.Context, unresolvedUpstreams []*dto.RawUpstream) ([]*job.Upstream, []*dto.RawUpstream, error) {
	me := errors.NewMultiError("external upstream resolution errors")
	var unknownUpstreams []*dto.RawUpstream
	var externalUpstreams []*job.Upstream
	for _, toBeResolvedUpstream := range unresolvedUpstreams {
		optimusUpstreams, err := e.fetchOptimusUpstreams(ctx, toBeResolvedUpstream)
		if err != nil || len(optimusUpstreams) == 0 {
			unknownUpstreams = append(unknownUpstreams, toBeResolvedUpstream)
			me.Append(err)
			continue
		}
		externalUpstreams = append(externalUpstreams, optimusUpstreams...)
	}
	return externalUpstreams, unknownUpstreams, errors.MultiToError(me)
}

func (e *extUpstreamResolver) fetchOptimusUpstreams(ctx context.Context, unresolvedUpstream *dto.RawUpstream) ([]*job.Upstream, error) {
	me := errors.NewMultiError("fetch external optimus job errors")
	var upstreams []*job.Upstream
	for _, manager := range e.optimusResourceManagers {
		deps, err := manager.GetOptimusUpstreams(ctx, unresolvedUpstream)
		if err != nil {
			me.Append(err)
			continue
		}
		upstreams = append(upstreams, deps...)
	}
	return upstreams, errors.MultiToError(me)
}

func NewTestExternalUpstreamResolver(
	optimusResourceManagers []resourcemanager.ResourceManager,
) ExternalUpstreamResolver {
	return &extUpstreamResolver{
		optimusResourceManagers: optimusResourceManagers,
	}
}
