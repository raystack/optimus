package service

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobService struct {
	repo JobRepository

	pluginService    PluginService
	upstreamResolver UpstreamResolver

	tenantDetailsGetter TenantDetailsGetter
}

func NewJobService(repo JobRepository, pluginService PluginService, upstreamResolver UpstreamResolver, tenantDetailsGetter TenantDetailsGetter) *JobService {
	return &JobService{repo: repo, pluginService: pluginService, upstreamResolver: upstreamResolver, tenantDetailsGetter: tenantDetailsGetter}
}

type PluginService interface {
	GenerateDestination(context.Context, *tenant.WithDetails, *job.Task) (job.ResourceURN, error)
	GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]job.ResourceURN, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type JobRepository interface {
	// TODO: remove `savedJobs` since the method's main purpose is to add, not to get
	Add(context.Context, []*job.Job) (savedJobs []*job.Job, err error)
	GetJobNameWithInternalUpstreams(context.Context, tenant.ProjectName, []job.Name) (map[job.Name][]*job.Upstream, error)
	ReplaceUpstreams(context.Context, []*job.WithUpstream) error
}

type UpstreamResolver interface {
	Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) (jobWithUpstreams []*job.WithUpstream, err error)
}

func (j JobService) Add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]job.Name, error) {
	me := errors.NewMultiError("add specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	generatedJobs, err := j.generateJobs(ctx, jobTenant, validatedSpecs)
	me.Append(err)

	addedJobNames, err := j.addJobs(ctx, jobTenant, generatedJobs)
	me.Append(err)

	return addedJobNames, errors.MultiToError(me)
}

func (j JobService) addJobs(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Job) ([]job.Name, error) {
	me := errors.NewMultiError("add jobs errors")

	addedJobs, err := j.repo.Add(ctx, jobs)
	me.Append(err)

	var addedJobNames []job.Name
	for _, addedJob := range addedJobs {
		addedJobNames = append(addedJobNames, addedJob.Spec().Name())
	}

	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, jobTenant.ProjectName(), addedJobs)
	me.Append(err)

	me.Append(j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams))

	return addedJobNames, errors.MultiToError(me)
}

func (JobService) getValidatedSpecs(jobs []*job.Spec) ([]*job.Spec, error) {
	me := errors.NewMultiError("spec validation errors")

	var validatedSpecs []*job.Spec
	for _, spec := range jobs {
		if err := spec.Validate(); err != nil {
			me.Append(err)
			continue
		}
		validatedSpecs = append(validatedSpecs, spec)
	}
	return validatedSpecs, errors.MultiToError(me)
}

func (j JobService) generateJobs(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]*job.Job, error) {
	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		return nil, err
	}

	me := errors.NewMultiError("generate jobs errors")
	var output []*job.Job
	for _, spec := range specs {
		destination, err := j.pluginService.GenerateDestination(ctx, tenantWithDetails, spec.Task())
		if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
			errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
			me.Append(errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
			continue
		}

		sources, err := j.pluginService.GenerateUpstreams(ctx, tenantWithDetails, spec, true)
		if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
			errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
			me.Append(errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
			continue
		}

		output = append(output, job.NewJob(jobTenant, spec, destination, sources))
	}
	return output, errors.MultiToError(me)
}
