package resolver

import (
	"fmt"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"golang.org/x/net/context"
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

	internalUpstreamStatic, err := i.resolveStaticUpstream(ctx, jobWithUnresolvedUpstream.Job().Tenant().ProjectName(), jobWithUnresolvedUpstream.Job().Spec().UpstreamSpec())
	me.Append(err)

	internalUpstream := mergeUpstreams(internalUpstreamInferred, internalUpstreamStatic)
	fullNameUpstreamMap := job.Upstreams(internalUpstream).ToFullNameAndUpstreamMap()
	resourceDestinationUpstreamMap := job.Upstreams(internalUpstream).ToResourceDestinationAndUpstreamMap()

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

	return job.NewWithUpstream(jobWithUnresolvedUpstream.Job(), upstreamResults), errors.MultiToError(me)
}

func (i internalUpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobsWithUnresolvedUpstream []*job.WithUpstream) ([]*job.WithUpstream, error) {
	jobNames := job.WithUpstreamList(jobsWithUnresolvedUpstream).GetSubjectJobNames()

	allInternalUpstreamMap, err := i.jobRepository.ResolveUpstreams(ctx, projectName, jobNames)
	if err != nil {
		errorMsg := fmt.Sprintf("unable to resolve upstream: %s", err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	var jobsWithMergedUpstream []*job.WithUpstream
	for _, jobWithUnresolvedUpstream := range jobsWithUnresolvedUpstream {
		internalUpstream := allInternalUpstreamMap[jobWithUnresolvedUpstream.Name()]
		jobInternalUpstreamMap := job.Upstreams(internalUpstream).ToFullNameAndUpstreamMap()

		var mergedUpstream []*job.Upstream
		for _, unresolvedUpstream := range jobWithUnresolvedUpstream.Upstreams() {
			if resolvedUpstream, ok := jobInternalUpstreamMap[unresolvedUpstream.FullName()]; ok {
				mergedUpstream = append(mergedUpstream, resolvedUpstream)
				continue
			}
			mergedUpstream = append(mergedUpstream, unresolvedUpstream)
		}
		jobsWithMergedUpstream = append(jobsWithMergedUpstream, job.NewWithUpstream(jobWithUnresolvedUpstream.Job(), mergedUpstream))
	}
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
	return internalUpstream, errors.MultiToError(me)
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
	return internalUpstream, errors.MultiToError(me)
}