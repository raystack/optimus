package resolver

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/writer"
)

const (
	ConcurrentTicketPerSec = 40
	ConcurrentLimit        = 600
)

type UpstreamResolver struct {
	jobRepository            JobRepository
	externalUpstreamResolver ExternalUpstreamResolver
	internalUpstreamResolver InternalUpstreamResolver
}

func NewUpstreamResolver(jobRepository JobRepository, externalUpstreamResolver ExternalUpstreamResolver, internalUpstreamResolver InternalUpstreamResolver) *UpstreamResolver {
	return &UpstreamResolver{jobRepository: jobRepository, externalUpstreamResolver: externalUpstreamResolver, internalUpstreamResolver: internalUpstreamResolver}
}

type ExternalUpstreamResolver interface {
	Resolve(ctx context.Context, jobWithUpstream *job.WithUpstream, lw writer.LogWriter) (*job.WithUpstream, error)
	BulkResolve(context.Context, []*job.WithUpstream, writer.LogWriter) ([]*job.WithUpstream, error)
}

type InternalUpstreamResolver interface {
	Resolve(context.Context, *job.WithUpstream) (*job.WithUpstream, error)
	BulkResolve(context.Context, tenant.ProjectName, []*job.WithUpstream) ([]*job.WithUpstream, error)
}

type JobRepository interface {
	ResolveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Upstream, error)

	GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error)
	GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
}

func (u UpstreamResolver) BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) ([]*job.WithUpstream, error) {
	me := errors.NewMultiError("bulk resolve jobs errors")

	var jobsWithUnresolvedUpstream []*job.WithUpstream
	for _, subjectJob := range jobs {
		jobWithUnresolvedUpstream, err := u.getJobWithUnresolvedUpstream(subjectJob)
		if err != nil {
			errorMsg := fmt.Sprintf("[%s] %s", subjectJob.Tenant().NamespaceName().String(), err.Error())
			logWriter.Write(writer.LogLevelError, errorMsg)
			me.Append(err)
		}
		jobsWithUnresolvedUpstream = append(jobsWithUnresolvedUpstream, jobWithUnresolvedUpstream)
	}

	jobsWithResolvedInternalUpstreams, err := u.internalUpstreamResolver.BulkResolve(ctx, projectName, jobsWithUnresolvedUpstream)
	if err != nil {
		errorMsg := fmt.Sprintf("unable to resolve upstream: %s", err.Error())
		logWriter.Write(writer.LogLevelError, errorMsg)
		me.Append(errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
		return nil, errors.MultiToError(me)
	}

	jobsWithResolvedExternalUpstreams, err := u.externalUpstreamResolver.BulkResolve(ctx, jobsWithResolvedInternalUpstreams, logWriter)
	me.Append(err)

	me.Append(u.getUnresolvedUpstreamsErrors(jobsWithResolvedExternalUpstreams, logWriter))

	return jobsWithResolvedExternalUpstreams, errors.MultiToError(me)
}

func (u UpstreamResolver) Resolve(ctx context.Context, subjectJob *job.Job, logWriter writer.LogWriter) ([]*job.Upstream, error) {
	me := errors.NewMultiError("upstream resolution errors")

	jobWithUnresolvedUpstream, err := u.getJobWithUnresolvedUpstream(subjectJob)
	me.Append(err)

	jobWithInternalUpstream, err := u.internalUpstreamResolver.Resolve(ctx, jobWithUnresolvedUpstream)
	me.Append(err)

	jobWithInternalExternalUpstream, err := u.externalUpstreamResolver.Resolve(ctx, jobWithInternalUpstream, logWriter)
	me.Append(err)

	return jobWithInternalExternalUpstream.Upstreams(), errors.MultiToError(me)
}

func (u UpstreamResolver) getJobWithUnresolvedUpstream(subjectJob *job.Job) (*job.WithUpstream, error) {
	unresolvedStaticUpstreams, err := u.getStaticUpstreamsToResolve(subjectJob.StaticUpstreamNames(), subjectJob.ProjectName())
	if err != nil {
		err = errors.InvalidArgument(job.EntityJob, fmt.Sprintf("failed to get static upstreams to resolve for job %s", subjectJob.GetName()))
	}

	unresolvedInferredUpstreams := u.getInferredUpstreamsToResolve(subjectJob.Sources())

	unresolvedUpstreams := mergeUpstreams(unresolvedInferredUpstreams, unresolvedStaticUpstreams)
	return job.NewWithUpstream(subjectJob, unresolvedUpstreams), err
}

func (UpstreamResolver) getInferredUpstreamsToResolve(sources []job.ResourceURN) []*job.Upstream {
	var unresolvedInferredUpstreams []*job.Upstream
	for _, source := range sources {
		unresolvedInferredUpstreams = append(unresolvedInferredUpstreams, job.NewUpstreamUnresolvedInferred(source))
	}
	return unresolvedInferredUpstreams
}

func (UpstreamResolver) getStaticUpstreamsToResolve(staticUpstreamNames []job.SpecUpstreamName, projectName tenant.ProjectName) ([]*job.Upstream, error) {
	var unresolvedStaticUpstreams []*job.Upstream
	me := errors.NewMultiError("get static upstream to resolve errors")

	for _, upstreamName := range staticUpstreamNames {
		jobUpstreamName, err := upstreamName.GetJobName()
		if err != nil {
			me.Append(err)
			continue
		}

		var projectUpstreamName tenant.ProjectName
		if upstreamName.IsWithProjectName() {
			projectUpstreamName, err = upstreamName.GetProjectName()
			if err != nil {
				me.Append(err)
				continue
			}
		} else {
			projectUpstreamName = projectName
		}

		unresolvedStaticUpstreams = append(unresolvedStaticUpstreams, job.NewUpstreamUnresolvedStatic(jobUpstreamName, projectUpstreamName))
	}
	return unresolvedStaticUpstreams, errors.MultiToError(me)
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
