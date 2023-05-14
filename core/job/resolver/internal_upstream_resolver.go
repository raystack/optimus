package resolver

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type internalUpstreamResolver struct {
	jobRepository JobRepository
}

func NewInternalUpstreamResolver(jobRepository JobRepository) *internalUpstreamResolver {
	return &internalUpstreamResolver{jobRepository: jobRepository}
}

func (i internalUpstreamResolver) Resolve(ctx context.Context, jobWithUnresolvedUpstream *job.WithUpstream) (*job.WithUpstream, error) {
	me := errors.NewMultiError("internal upstream resolution errors")

	internalUpstreamInferred, err := i.resolveInferredUpstream(ctx, jobWithUnresolvedUpstream.Job().Sources())
	me.Append(err)

	var internalUpstreamStatic []*job.Upstream
	if staticUpstreamSpec := jobWithUnresolvedUpstream.Job().Spec().UpstreamSpec(); staticUpstreamSpec != nil {
		internalUpstreamStatic, err = i.resolveStaticUpstream(ctx, jobWithUnresolvedUpstream.Job().Tenant().ProjectName(), staticUpstreamSpec)
		me.Append(err)
	}

	internalUpstream := mergeUpstreams(internalUpstreamInferred, internalUpstreamStatic)
	distinctInternalUpstream := job.Upstreams(internalUpstream).Deduplicate()
	fullNameUpstreamMap := job.Upstreams(distinctInternalUpstream).ToFullNameAndUpstreamMap()
	resourceDestinationUpstreamMap := job.Upstreams(distinctInternalUpstream).ToResourceDestinationAndUpstreamMap()

	var upstreamResults []*job.Upstream
	for _, unresolvedUpstream := range jobWithUnresolvedUpstream.Upstreams() {
		if resolvedUpstream, ok := resourceDestinationUpstreamMap[unresolvedUpstream.Resource().String()]; ok {
			upstreamResults = append(upstreamResults, resolvedUpstream)
			continue
		}
		if resolvedUpstream, ok := fullNameUpstreamMap[unresolvedUpstream.FullName()]; ok {
			upstreamResults = append(upstreamResults, resolvedUpstream)
			continue
		}
		upstreamResults = append(upstreamResults, unresolvedUpstream)
	}

	distinctUpstreams := job.Upstreams(upstreamResults).Deduplicate()
	return job.NewWithUpstream(jobWithUnresolvedUpstream.Job(), distinctUpstreams), me.ToErr()
}

func (i internalUpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobsWithUnresolvedUpstream []*job.WithUpstream) ([]*job.WithUpstream, error) {
	jobNames := job.WithUpstreams(jobsWithUnresolvedUpstream).GetSubjectJobNames()

	allInternalUpstreamMap, err := i.jobRepository.ResolveUpstreams(ctx, projectName, jobNames)
	if err != nil {
		errorMsg := fmt.Sprintf("unable to resolve upstream: %s", err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	jobsWithMergedUpstream := job.WithUpstreams(jobsWithUnresolvedUpstream).MergeWithResolvedUpstreams(allInternalUpstreamMap)
	return jobsWithMergedUpstream, nil
}

func (i internalUpstreamResolver) resolveInferredUpstream(ctx context.Context, sources []job.ResourceURN) ([]*job.Upstream, error) {
	var internalUpstream []*job.Upstream
	me := errors.NewMultiError("resolve internal inferred upstream errors")
	for _, source := range sources {
		jobUpstreams, err := i.jobRepository.GetAllByResourceDestination(ctx, source)
		me.Append(err)
		if len(jobUpstreams) == 0 {
			continue
		}
		upstream := job.NewUpstreamResolved(jobUpstreams[0].Spec().Name(), "", jobUpstreams[0].Destination(), jobUpstreams[0].Tenant(), job.UpstreamTypeInferred, jobUpstreams[0].Spec().Task().Name(), false)
		internalUpstream = append(internalUpstream, upstream)
	}
	return internalUpstream, me.ToErr()
}

func (i internalUpstreamResolver) resolveStaticUpstream(ctx context.Context, projectName tenant.ProjectName, upstreamSpec *job.UpstreamSpec) ([]*job.Upstream, error) {
	var internalUpstream []*job.Upstream
	me := errors.NewMultiError("resolve internal static upstream errors")
	for _, upstreamName := range upstreamSpec.UpstreamNames() {
		upstreamJobName, err := upstreamName.GetJobName()
		if err != nil {
			me.Append(err)
			continue
		}
		jobUpstream, err := i.jobRepository.GetByJobName(ctx, projectName, upstreamJobName)
		if err != nil || jobUpstream == nil {
			me.Append(err)
			continue
		}
		upstream := job.NewUpstreamResolved(upstreamJobName, "", jobUpstream.Destination(), jobUpstream.Tenant(), job.UpstreamTypeStatic, jobUpstream.Spec().Task().Name(), false)
		internalUpstream = append(internalUpstream, upstream)
	}
	return internalUpstream, me.ToErr()
}
