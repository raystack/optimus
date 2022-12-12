package resolver

import (
	"context"
	"fmt"

	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/ext/resourcemanager"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/writer"
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
	me := errors.NewMultiError(fmt.Sprintf("external upstream resolution errors for job %s", jobWithUpstream.Name().String()))
	unresolvedUpstreamList := jobWithUpstream.GetUnresolvedUpstreams()
	var mergedUpstream []*job.Upstream
	for _, unresolvedUpstream := range unresolvedUpstreamList {
		externalUpstream, err := e.fetchOptimusUpstreams(ctx, unresolvedUpstream)
		if err != nil || len(externalUpstream) == 0 {
			mergedUpstream = append(mergedUpstream, unresolvedUpstream)
			me.Append(err)
			continue
		}
		mergedUpstream = append(mergedUpstream, externalUpstream...)
	}
	if len(me.Errors) > 0 {
		lw.Write(writer.LogLevelError, errors.MultiToError(me).Error())
	} else {
		lw.Write(writer.LogLevelDebug, fmt.Sprintf("resolved job %s upstream from external", jobWithUpstream.Name().String()))
	}
	return job.NewWithUpstream(jobWithUpstream.Job(), mergedUpstream), errors.MultiToError(me)
}

func (e *extUpstreamResolver) BulkResolve(ctx context.Context, jobsWithUpstream []*job.WithUpstream, lw writer.LogWriter) ([]*job.WithUpstream, error) {
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
