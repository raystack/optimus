package resolver

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type UpstreamResolver struct {
	jobRepository            JobRepository
	externalUpstreamResolver ExternalUpstreamResolver
}

func NewUpstreamResolver(jobRepository JobRepository, externalUpstreamResolver ExternalUpstreamResolver) *UpstreamResolver {
	return &UpstreamResolver{jobRepository: jobRepository, externalUpstreamResolver: externalUpstreamResolver}
}

type ExternalUpstreamResolver interface {
	FetchExternalUpstreams(ctx context.Context, unresolvedUpstreams []*dto.RawUpstream) ([]*job.Upstream, []*dto.RawUpstream, error)
}

type JobRepository interface {
	Add(ctx context.Context, jobs []*job.Job) (savedJobs []*job.Job, jobErrors error, err error)
	GetJobNameWithInternalUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error)
}

func (u UpstreamResolver) Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("resolve jobs errors")

	// get internal inferred and static upstreams
	jobNames := job.Jobs(jobs).GetJobNames()
	jobsWithInternalUpstreams, err := u.jobRepository.GetJobNameWithInternalUpstreams(ctx, projectName, jobNames)
	if err != nil {
		return nil, err
	}

	// merge with external upstreams
	jobsWithAllUpstreams, err := u.getJobsWithAllUpstreams(ctx, jobs, jobsWithInternalUpstreams)
	me.Append(err)

	me.Append(u.getUnresolvedUpstreamsErrors(jobsWithAllUpstreams))
	return jobsWithAllUpstreams, errors.MultiToError(me)
}

func (u UpstreamResolver) getJobsWithAllUpstreams(ctx context.Context, jobs []*job.Job, jobsWithInternalUpstreams map[job.Name][]*job.Upstream) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("get jobs with all upstreams errors")

	var jobsWithAllUpstreams []*job.WithUpstream
	for _, jobEntity := range jobs {
		var allUpstreams []*job.Upstream

		// get internal upstreams
		internalUpstreams := jobsWithInternalUpstreams[jobEntity.Spec().Name()]
		allUpstreams = append(allUpstreams, internalUpstreams...)

		// try to resolve upstreams from external
		unresolvedUpstreams := u.identifyUnresolvedUpstreams(internalUpstreams, jobEntity)
		externalUpstreams, unresolvedUpstreams, err := u.externalUpstreamResolver.FetchExternalUpstreams(ctx, unresolvedUpstreams)
		if err != nil {
			me.Append(err)
		}
		allUpstreams = append(allUpstreams, externalUpstreams...)

		// include unresolved upstreams
		for _, upstream := range unresolvedUpstreams {
			// allow empty upstreamName and upstreamResourceURN
			upstreamName, _ := job.NameFrom(upstream.JobName)
			upstreamResourceURN, _ := job.ResourceURNFrom(upstream.ResourceURN)
			allUpstreams = append(allUpstreams, job.NewUpstreamUnresolved(upstreamName, upstreamResourceURN, upstream.ProjectName))
		}

		jobWithAllUpstreams := job.NewWithUpstream(jobEntity, allUpstreams)
		jobsWithAllUpstreams = append(jobsWithAllUpstreams, jobWithAllUpstreams)
	}
	return jobsWithAllUpstreams, errors.MultiToError(me)
}

func (u UpstreamResolver) identifyUnresolvedUpstreams(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) (unresolvedUpstreams []*dto.RawUpstream) {
	unresolvedStaticUpstreams := u.identifyUnresolvedStaticUpstream(resolvedUpstreams, jobEntity)
	unresolvedUpstreams = append(unresolvedUpstreams, unresolvedStaticUpstreams...)

	unresolvedInferredUpstreams := u.identifyUnresolvedInferredUpstreams(resolvedUpstreams, jobEntity)
	unresolvedUpstreams = append(unresolvedUpstreams, unresolvedInferredUpstreams...)

	return unresolvedUpstreams
}

func (UpstreamResolver) identifyUnresolvedInferredUpstreams(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) []*dto.RawUpstream {
	var unresolvedInferredUpstreams []*dto.RawUpstream
	resolvedUpstreamDestinationMap := job.Upstreams(resolvedUpstreams).ToUpstreamDestinationMap()
	for _, source := range jobEntity.Sources() {
		if !resolvedUpstreamDestinationMap[source] {
			unresolvedInferredUpstreams = append(unresolvedInferredUpstreams, &dto.RawUpstream{
				ResourceURN: source.String(),
			})
		}
	}
	return unresolvedInferredUpstreams
}

func (UpstreamResolver) identifyUnresolvedStaticUpstream(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) []*dto.RawUpstream {
	var unresolvedStaticUpstreams []*dto.RawUpstream
	resolvedUpstreamFullNameMap := job.Upstreams(resolvedUpstreams).ToUpstreamFullNameMap()
	for _, upstreamName := range jobEntity.StaticUpstreamNames() {
		jobUpstreamName, _ := upstreamName.GetJobName()

		var projectUpstreamName tenant.ProjectName
		if upstreamName.IsWithProjectName() {
			projectUpstreamName, _ = upstreamName.GetProjectName()
		} else {
			projectUpstreamName = jobEntity.ProjectName()
		}

		fullUpstreamName := jobEntity.ProjectName().String() + "/" + upstreamName.String()
		if !resolvedUpstreamFullNameMap[fullUpstreamName] {
			unresolvedStaticUpstreams = append(unresolvedStaticUpstreams, &dto.RawUpstream{
				ProjectName: projectUpstreamName.String(),
				JobName:     jobUpstreamName.String(),
			})
		}
	}
	return unresolvedStaticUpstreams
}

func (UpstreamResolver) getUnresolvedUpstreamsErrors(jobsWithUpstreams []*job.WithUpstream) error {
	me := errors.NewMultiError("unresolved upstreams errors")
	for _, jobWithUpstreams := range jobsWithUpstreams {
		for _, unresolvedUpstream := range jobWithUpstreams.GetUnresolvedUpstreams() {
			if unresolvedUpstream.Type() == job.UpstreamTypeStatic {
				errMsg := fmt.Sprintf("[%s] error: %s unknown upstream", jobWithUpstreams.Name().String(), unresolvedUpstream.Name())
				me.Append(errors.NewError(errors.ErrNotFound, job.EntityJob, errMsg))
			}
		}
	}
	return errors.MultiToError(me)
}
