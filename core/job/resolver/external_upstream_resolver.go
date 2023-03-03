package resolver

import (
	"context"
	"fmt"

	"github.com/kushsharma/parallel"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/ext/resourcemanager"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/writer"
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

func (e *extUpstreamResolver) Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error) {
	me := errors.NewMultiError(fmt.Sprintf("[%s] external upstream resolution errors for job %s", jobWithUpstream.Job().Tenant().NamespaceName().String(), jobWithUpstream.Name().String()))
	mergedUpstreams := jobWithUpstream.GetResolvedUpstreams()
	unresolvedUpstreams := jobWithUpstream.GetUnresolvedUpstreams()
	resolvedExternally := false
	for _, unresolvedUpstream := range unresolvedUpstreams {
		externalUpstream, err := e.fetchOptimusUpstreams(ctx, unresolvedUpstream)
		if err != nil || len(externalUpstream) == 0 {
			mergedUpstreams = append(mergedUpstreams, unresolvedUpstream)
			me.Append(err)
			continue
		}
		mergedUpstreams = append(mergedUpstreams, externalUpstream...)
		resolvedExternally = true
	}
	if len(me.Errors) > 0 {
		lw.Write(writer.LogLevelError, errors.MultiToError(me).Error())
	}
	if resolvedExternally {
		lw.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] resolved job %s upstream from external", jobWithUpstream.Job().Tenant().NamespaceName().String(), jobWithUpstream.Name().String()))
	}
	return job.NewWithUpstream(jobWithUpstream.Job(), mergedUpstreams), errors.MultiToError(me)
}

func (e *extUpstreamResolver) BulkResolve(ctx context.Context, jobsWithUpstream []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error) {
	if len(e.optimusResourceManagers) == 0 {
		return jobsWithUpstream, nil
	}

	me := errors.NewMultiError("external upstream resolution errors")

	var jobsWithAllUpstream []*job.WithUpstream
	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobWithUpstream := range jobsWithUpstream {
		runner.Add(func(currentJobWithUpstream *job.WithUpstream, lw writer.LogWriter) func() (interface{}, error) {
			return func() (interface{}, error) {
				return e.Resolve(ctx, currentJobWithUpstream, lw)
			}
		}(jobWithUpstream, lw))
	}

	for _, result := range runner.Run() {
		if result.Val != nil {
			specVal := result.Val.(*job.WithUpstream)
			jobsWithAllUpstream = append(jobsWithAllUpstream, specVal)
		}
		me.Append(result.Err)
	}

	return jobsWithAllUpstream, errors.MultiToError(me)
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
