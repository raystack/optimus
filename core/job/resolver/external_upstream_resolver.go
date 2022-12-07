package resolver

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job"
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
	GetOptimusUpstreams(ctx context.Context, unresolvedDependency *job.Upstream) ([]*job.Upstream, error)
}

func (e *extUpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job, internalUpstream []*job.Upstream) ([]*job.Upstream, []*job.Upstream, error) {
	me := errors.NewMultiError("external upstream resolution errors")

	var unknownUpstreams []*job.Upstream
	var externalUpstreams []*job.Upstream

	upstreamsToResolve := e.getUpstreamsToResolve(internalUpstream, subjectJob)
	for _, toBeResolvedUpstream := range upstreamsToResolve {
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

func (e *extUpstreamResolver) getUpstreamsToResolve(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) (upstreamsToResolve []*job.Upstream) {
	unresolvedStaticUpstreams := e.getStaticUpstreamsToResolve(resolvedUpstreams, jobEntity.StaticUpstreamNames(), jobEntity.ProjectName())
	upstreamsToResolve = append(upstreamsToResolve, unresolvedStaticUpstreams...)

	unresolvedInferredUpstreams := e.getInferredUpstreamsToResolve(resolvedUpstreams, jobEntity.Sources())
	upstreamsToResolve = append(upstreamsToResolve, unresolvedInferredUpstreams...)

	return upstreamsToResolve
}

func (extUpstreamResolver) getInferredUpstreamsToResolve(resolvedUpstreams []*job.Upstream, sources []job.ResourceURN) []*job.Upstream {
	var unresolvedInferredUpstreams []*job.Upstream
	resolvedUpstreamDestinationMap := job.Upstreams(resolvedUpstreams).ToUpstreamDestinationMap()
	for _, source := range sources {
		if !resolvedUpstreamDestinationMap[source] {
			unresolvedInferredUpstreams = append(unresolvedInferredUpstreams, job.NewUpstreamUnresolvedInferred(source))
		}
	}
	return unresolvedInferredUpstreams
}

func (extUpstreamResolver) getStaticUpstreamsToResolve(resolvedUpstreams []*job.Upstream, staticUpstreamNames []job.SpecUpstreamName, projectName tenant.ProjectName) []*job.Upstream {
	var unresolvedStaticUpstreams []*job.Upstream
	resolvedUpstreamFullNameMap := job.Upstreams(resolvedUpstreams).ToUpstreamFullNameMap()
	for _, upstreamName := range staticUpstreamNames {
		jobUpstreamName, _ := upstreamName.GetJobName()

		var projectUpstreamName tenant.ProjectName
		if upstreamName.IsWithProjectName() {
			projectUpstreamName, _ = upstreamName.GetProjectName()
		} else {
			projectUpstreamName = projectName
		}

		fullUpstreamName := projectName.String() + "/" + upstreamName.String()
		if !resolvedUpstreamFullNameMap[fullUpstreamName] {
			unresolvedStaticUpstreams = append(unresolvedStaticUpstreams, job.NewUpstreamUnresolvedStatic(jobUpstreamName, projectUpstreamName))
		}
	}
	return unresolvedStaticUpstreams
}

func (e *extUpstreamResolver) fetchOptimusUpstreams(ctx context.Context, unresolvedUpstream *job.Upstream) ([]*job.Upstream, error) {
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
