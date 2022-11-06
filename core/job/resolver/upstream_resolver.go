package resolver

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"

	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
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

func (d UpstreamResolver) Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) (jobsWithAllUpstreams []*job.WithUpstream, upstreamErrors error, err error) {
	// get internal inferred and static upstreams
	jobNames := job.Jobs(jobs).GetJobNames()
	jobsWithInternalUpstreams, err := d.jobRepository.GetJobNameWithInternalUpstreams(ctx, projectName, jobNames)
	if err != nil {
		return nil, nil, err
	}

	// merge with external upstreams
	jobsWithAllUpstreams, getUpstreamErr := d.getJobsWithAllUpstreams(ctx, jobs, jobsWithInternalUpstreams)
	if getUpstreamErr != nil {
		upstreamErrors = multierror.Append(upstreamErrors, getUpstreamErr)
	}
	if unresolvedUpstreamErrors := d.getUnresolvedUpstreamsErrors(jobsWithAllUpstreams); unresolvedUpstreamErrors != nil {
		upstreamErrors = multierror.Append(upstreamErrors, unresolvedUpstreamErrors)
	}
	return jobsWithAllUpstreams, upstreamErrors, nil
}

func (d UpstreamResolver) getJobsWithAllUpstreams(ctx context.Context, jobs []*job.Job, jobsWithInternalUpstreams map[job.Name][]*job.Upstream) ([]*job.WithUpstream, error) {
	var jobsWithAllUpstreams []*job.WithUpstream
	var allErrors error

	for _, jobEntity := range jobs {
		var allUpstreams []*job.Upstream

		// get internal upstreams
		internalUpstreams := jobsWithInternalUpstreams[jobEntity.Spec().Name()]
		allUpstreams = append(allUpstreams, internalUpstreams...)

		// try to resolve upstreams from external
		unresolvedUpstreams := d.identifyUnresolvedUpstreams(internalUpstreams, jobEntity)
		externalUpstreams, unresolvedUpstreams, err := d.externalUpstreamResolver.FetchExternalUpstreams(ctx, unresolvedUpstreams)
		if err != nil {
			allErrors = multierror.Append(allErrors, err)
		}
		allUpstreams = append(allUpstreams, externalUpstreams...)

		// include unresolved upstreams
		for _, upstream := range unresolvedUpstreams {
			allUpstreams = append(allUpstreams, job.NewUpstreamUnresolved(upstream.JobName, upstream.ResourceURN, upstream.ProjectName))
		}

		jobWithAllUpstreams := job.NewWithUpstream(jobEntity, allUpstreams)
		jobsWithAllUpstreams = append(jobsWithAllUpstreams, jobWithAllUpstreams)
	}
	return jobsWithAllUpstreams, allErrors
}

func (d UpstreamResolver) identifyUnresolvedUpstreams(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) (unresolvedUpstreams []*dto.RawUpstream) {
	unresolvedStaticUpstreams := d.identifyUnresolvedStaticUpstream(resolvedUpstreams, jobEntity)
	unresolvedUpstreams = append(unresolvedUpstreams, unresolvedStaticUpstreams...)

	unresolvedInferredUpstreams := d.identifyUnresolvedInferredUpstreams(resolvedUpstreams, jobEntity)
	unresolvedUpstreams = append(unresolvedUpstreams, unresolvedInferredUpstreams...)

	return unresolvedUpstreams
}

func (d UpstreamResolver) identifyUnresolvedInferredUpstreams(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) []*dto.RawUpstream {
	var unresolvedInferredUpstreams []*dto.RawUpstream
	resolvedUpstreamDestinationMap := job.Upstreams(resolvedUpstreams).ToUpstreamDestinationMap()
	for _, source := range jobEntity.Sources() {
		if !resolvedUpstreamDestinationMap[source] {
			unresolvedInferredUpstreams = append(unresolvedInferredUpstreams, &dto.RawUpstream{
				ResourceURN: source,
			})
		}
	}
	return unresolvedInferredUpstreams
}

func (d UpstreamResolver) identifyUnresolvedStaticUpstream(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) []*dto.RawUpstream {
	var unresolvedStaticUpstreams []*dto.RawUpstream
	resolvedUpstreamFullNameMap := job.Upstreams(resolvedUpstreams).ToUpstreamFullNameMap()
	for _, upstreamName := range jobEntity.StaticUpstreamNames() {
		var projectUpstreamName, jobUpstreamName string

		if strings.Contains(upstreamName, "/") {
			projectUpstreamName = strings.Split(upstreamName, "/")[0]
			jobUpstreamName = strings.Split(upstreamName, "/")[1]
		} else {
			projectUpstreamName = jobEntity.ProjectName().String()
			jobUpstreamName = upstreamName
		}

		fullUpstreamName := jobEntity.ProjectName().String() + "/" + upstreamName
		if !resolvedUpstreamFullNameMap[fullUpstreamName] {
			unresolvedStaticUpstreams = append(unresolvedStaticUpstreams, &dto.RawUpstream{
				ProjectName: projectUpstreamName,
				JobName:     jobUpstreamName,
			})
		}
	}
	return unresolvedStaticUpstreams
}

func (UpstreamResolver) getUnresolvedUpstreamsErrors(jobsWithUpstreams []*job.WithUpstream) error {
	var upstreamErr error
	for _, jobWithUpstreams := range jobsWithUpstreams {
		for _, unresolvedUpstream := range jobWithUpstreams.GetUnresolvedUpstreams() {
			if unresolvedUpstream.Type() == job.UpstreamTypeStatic {
				errMsg := fmt.Sprintf("[%s] error: %s unknown upstream", jobWithUpstreams.Name().String(), unresolvedUpstream.Name())
				upstreamErr = multierror.Append(upstreamErr, errors.NewError(errors.ErrNotFound, job.EntityJob, errMsg))
			}
		}
	}
	return upstreamErr
}
