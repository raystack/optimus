package resolver

import (
	"context"
	"fmt"

	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
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
	Resolve(ctx context.Context, subjectJob *job.Job, internalUpstream []*job.Upstream) ([]*job.Upstream, []*job.Upstream, error)
}

type JobRepository interface {
	ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error)

	GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error)
	GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
}

func (u UpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("bulk resolve jobs errors")

	jobNames := job.Jobs(jobs).GetJobNames()
	allInternalUpstream, err := u.jobRepository.ResolveUpstreams(ctx, projectName, jobNames)
	if err != nil {
		errorMsg := fmt.Sprintf("unable to resolve upstream: %s", err.Error())
		logWriter.Write(writer.LogLevelError, errorMsg)
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, jobEntity := range jobs {
		runner.Add(func(currentJob *job.Job, lw writer.LogWriter) func() (interface{}, error) {
			return u.getJobWithAllUpstream(ctx, currentJob, allInternalUpstream[currentJob.Spec().Name()], lw)
		}(jobEntity, logWriter))
	}

	var jobsWithAllUpstreams []*job.WithUpstream
	for _, result := range runner.Run() {
		if result.Val != nil {
			specVal := result.Val.(*job.WithUpstream)
			jobsWithAllUpstreams = append(jobsWithAllUpstreams, specVal)
		}
		me.Append(result.Err)
	}

	me.Append(u.getUnresolvedUpstreamsErrors(jobsWithAllUpstreams, logWriter))

	return jobsWithAllUpstreams, errors.MultiToError(me)
}

func (u UpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job) ([]*job.Upstream, error) {
	me := errors.NewMultiError("upstream resolution errors")

	internalUpstream, err := u.resolveFromInternal(ctx, subjectJob)
	me.Append(err)

	externalUpstreams, unresolvedUpstreams, err := u.externalUpstreamResolver.Resolve(ctx, subjectJob, internalUpstream)
	me.Append(err)

	return mergeUpstreams(internalUpstream, externalUpstreams, unresolvedUpstreams), errors.MultiToError(me)
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
		upstream := job.NewUpstreamResolved(jobUpstreams[0].Spec().Name(), "", jobUpstreams[0].Destination(), jobUpstreams[0].Tenant(), job.UpstreamTypeInferred, jobUpstreams[0].Spec().Task().Name(), false)
		internalUpstream = append(internalUpstream, upstream)
	}
	for _, upstreamName := range subjectJob.Spec().Upstream().UpstreamNames() {
		upstreamJobName, err := upstreamName.GetJobName()
		if err != nil {
			me.Append(err)
			continue
		}
		jobUpstream, err := u.jobRepository.GetByJobName(ctx, subjectJob.Tenant().ProjectName(), upstreamJobName)
		if err != nil || jobUpstream == nil {
			me.Append(err)
			continue
		}
		upstream := job.NewUpstreamResolved(upstreamJobName, "", jobUpstream.Destination(), jobUpstream.Tenant(), job.UpstreamTypeStatic, jobUpstream.Spec().Task().Name(), false)
		internalUpstream = append(internalUpstream, upstream)
	}
	return internalUpstream, errors.MultiToError(me)
}

func (u UpstreamResolver) getJobWithAllUpstream(ctx context.Context, currentJob *job.Job, internalUpstream []*job.Upstream, lw writer.LogWriter) func() (interface{}, error) {
	return func() (interface{}, error) {
		var wrappedErr error
		externalUpstreams, unresolvedUpstreams, err := u.externalUpstreamResolver.Resolve(ctx, currentJob, internalUpstream)
		if err != nil {
			errorMsg := fmt.Sprintf("job %s upstream resolution failed: %s", currentJob.Spec().Name().String(), err.Error())
			wrappedErr = errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
			lw.Write(writer.LogLevelError, fmt.Sprintf("[%s] %s", currentJob.Tenant().NamespaceName().String(), errorMsg))
		} else {
			lw.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] job %s upstream resolved", currentJob.Tenant().NamespaceName().String(), currentJob.Spec().Name().String()))
		}

		allUpstreams := mergeUpstreams(internalUpstream, externalUpstreams, unresolvedUpstreams)
		return job.NewWithUpstream(currentJob, allUpstreams), wrappedErr
	}
}

func (UpstreamResolver) getUnresolvedUpstreamsErrors(jobsWithUpstreams []*job.WithUpstream, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("unresolved upstreams errors")
	for _, jobWithUpstreams := range jobsWithUpstreams {
		for _, unresolvedUpstream := range jobWithUpstreams.GetUnresolvedUpstreams() {
			if unresolvedUpstream.Type() == job.UpstreamTypeStatic {
				errMsg := fmt.Sprintf("[%s] found unknown upstream for job %s: %s", jobWithUpstreams.Job().Tenant().NamespaceName().String(), jobWithUpstreams.Name().String(), unresolvedUpstream.FullName())
				logWriter.Write(writer.LogLevelError, errMsg)
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
