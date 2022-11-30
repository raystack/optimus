package resolver

import (
	"context"
	"fmt"
	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	ConcurrentTicketPerSec = 40
	ConcurrentLimit        = 600
)

type UpstreamResolver struct {
	jobRepository            JobRepository
	externalUpstreamResolver ExternalUpstreamResolver
}

func NewUpstreamResolver(jobRepository JobRepository, externalUpstreamResolver ExternalUpstreamResolver) *UpstreamResolver {
	return &UpstreamResolver{jobRepository: jobRepository, externalUpstreamResolver: externalUpstreamResolver}
}

type ExternalUpstreamResolver interface {
	Resolve(ctx context.Context, unresolvedUpstreams []*dto.RawUpstream) ([]*job.Upstream, []*job.Upstream, error)
}

type JobRepository interface {
	GetJobNameWithInternalUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error)

	GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error)
	GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
}

func (u UpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("bulk resolve jobs errors")

	// get internal inferred and static upstreams in bulk
	jobNames := job.Jobs(jobs).GetJobNames()
	jobsWithInternalUpstreams, err := u.jobRepository.GetJobNameWithInternalUpstreams(ctx, projectName, jobNames)
	if err != nil {
		return nil, err
	}

	// merge with external upstreams
	jobsWithAllUpstreams, err := u.getJobsWithAllUpstreams(ctx, jobs, jobsWithInternalUpstreams, logWriter)
	me.Append(err)

	me.Append(u.getUnresolvedUpstreamsErrors(jobsWithAllUpstreams))
	return jobsWithAllUpstreams, errors.MultiToError(me)
}

func (u UpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job) ([]*job.Upstream, error) {
	me := errors.NewMultiError("upstream resolution errors")

	internalUpstream, err := u.resolveFromInternal(ctx, subjectJob)
	me.Append(err)

	upstreamsToResolve := u.getUpstreamsToResolve(internalUpstream, subjectJob)
	externalUpstreams, unresolvedUpstreams, err := u.externalUpstreamResolver.Resolve(ctx, upstreamsToResolve)
	me.Append(err)

	return mergeUpstreams(internalUpstream, externalUpstreams, unresolvedUpstreams), errors.MultiToError(err)
}

func (u UpstreamResolver) resolveFromInternal(ctx context.Context, subjectJob *job.Job) ([]*job.Upstream, error) {
	var internalUpstream []*job.Upstream
	me := errors.NewMultiError("internal upstream resolution errors")
	for _, source := range subjectJob.Sources() {
		jobUpstreams, err := u.jobRepository.GetAllByResourceDestination(ctx, source)
		me.Append(err)
		if len(jobUpstreams) == 0 {
			continue
		}
		upstream, err := job.NewUpstreamResolved(jobUpstreams[0].Spec().Name(), "", jobUpstreams[0].Destination(), jobUpstreams[0].Tenant(), "inferred", jobUpstreams[0].Spec().Task().Name(), false)
		if err != nil {
			me.Append(err)
			continue
		}
		internalUpstream = append(internalUpstream, upstream)
	}
	for _, upstreamName := range subjectJob.Spec().Upstream().UpstreamNames() {
		upstreamJobName, err := upstreamName.GetJobName()
		if err != nil {
			me.Append(err)
			continue
		}
		jobUpstream, err := u.jobRepository.GetByJobName(ctx, subjectJob.Tenant().ProjectName(), upstreamJobName)
		me.Append(err)
		if jobUpstream == nil {
			continue
		}
		upstream, err := job.NewUpstreamResolved(upstreamJobName, "", jobUpstream.Destination(), jobUpstream.Tenant(), "static", jobUpstream.Spec().Task().Name(), false)
		if err != nil {
			me.Append(err)
			continue
		}
		internalUpstream = append(internalUpstream, upstream)
	}
	return internalUpstream, errors.MultiToError(me)
}

func (u UpstreamResolver) getJobsWithAllUpstreams(ctx context.Context, jobs []*job.Job, jobsWithInternalUpstreams map[job.Name][]*job.Upstream, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("get jobs with all upstreams errors")

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobEntity := range jobs {
		runner.Add(func(currentJob *job.Job, lw writer.LogWriter) func() (interface{}, error) {
			return func() (interface{}, error) {
				internalUpstreams := jobsWithInternalUpstreams[currentJob.Spec().Name()]
				upstreamsToResolve := u.getUpstreamsToResolve(internalUpstreams, currentJob)
				externalUpstreams, unresolvedUpstreams, err := u.externalUpstreamResolver.Resolve(ctx, upstreamsToResolve)
				if err != nil {
					errorMsg := fmt.Sprintf("job %s upstream resolution failed: %s", currentJob.Spec().Name().String(), err.Error())
					logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] %s", currentJob.Tenant().NamespaceName().String(), errorMsg))
				}

				allUpstreams := mergeUpstreams(internalUpstreams, externalUpstreams, unresolvedUpstreams)

				logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] job %s upstream resolved", currentJob.Tenant().NamespaceName().String(), currentJob.Spec().Name().String()))
				return job.NewWithUpstream(currentJob, allUpstreams), err
			}
		}(jobEntity, logWriter))
	}

	var jobsWithAllUpstreams []*job.WithUpstream
	for _, result := range runner.Run() {
		if result.Err != nil {
			me.Append(result.Err)
		}
		if result.Val != nil {
			specVal := result.Val.(*job.WithUpstream)
			jobsWithAllUpstreams = append(jobsWithAllUpstreams, specVal)
		}
	}

	return jobsWithAllUpstreams, errors.MultiToError(me)
}

func (u UpstreamResolver) getUpstreamsToResolve(resolvedUpstreams []*job.Upstream, jobEntity *job.Job) (upstreamsToResolve []*dto.RawUpstream) {
	unresolvedStaticUpstreams := u.getStaticUpstreamsToResolve(resolvedUpstreams, jobEntity.StaticUpstreamNames(), jobEntity.ProjectName())
	upstreamsToResolve = append(upstreamsToResolve, unresolvedStaticUpstreams...)

	unresolvedInferredUpstreams := u.getInferredUpstreamsToResolve(resolvedUpstreams, jobEntity.Sources())
	upstreamsToResolve = append(upstreamsToResolve, unresolvedInferredUpstreams...)

	return upstreamsToResolve
}

func (UpstreamResolver) getInferredUpstreamsToResolve(resolvedUpstreams []*job.Upstream, sources []job.ResourceURN) []*dto.RawUpstream {
	var unresolvedInferredUpstreams []*dto.RawUpstream
	resolvedUpstreamDestinationMap := job.Upstreams(resolvedUpstreams).ToUpstreamDestinationMap()
	for _, source := range sources {
		if !resolvedUpstreamDestinationMap[source] {
			unresolvedInferredUpstreams = append(unresolvedInferredUpstreams, &dto.RawUpstream{
				ResourceURN: source.String(),
			})
		}
	}
	return unresolvedInferredUpstreams
}

func (UpstreamResolver) getStaticUpstreamsToResolve(resolvedUpstreams []*job.Upstream, staticUpstreamNames []job.SpecUpstreamName, projectName tenant.ProjectName) []*dto.RawUpstream {
	var unresolvedStaticUpstreams []*dto.RawUpstream
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

func mergeUpstreams(upstreamGroups ...[]*job.Upstream) []*job.Upstream {
	var allUpstreams []*job.Upstream
	for _, group := range upstreamGroups {
		allUpstreams = append(allUpstreams, group...)
	}
	return allUpstreams
}
