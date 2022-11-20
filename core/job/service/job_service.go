package service

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service/filter"
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
	Add(context.Context, []*job.Job) (addedJobs []*job.Job, err error)
	Update(context.Context, []*job.Job) (updatedJobs []*job.Job, err error)
	GetJobNameWithInternalUpstreams(context.Context, tenant.ProjectName, []job.Name) (map[job.Name][]*job.Upstream, error)
	ReplaceUpstreams(context.Context, []*job.WithUpstream) error

	GetDownstreamFullNames(context.Context, tenant.ProjectName, job.Name) ([]job.FullName, error)
	Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error

	GetByJobName(ctx context.Context, projectName, jobName string) (*job.Spec, error)
	GetAllByProjectName(ctx context.Context, projectName string) ([]*job.Spec, error)
	GetAllByResourceDestination(ctx context.Context, resourceDestination string) ([]*job.Spec, error)
	GetAllSpecsByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Spec, error)
}

type UpstreamResolver interface {
	Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) (jobWithUpstreams []*job.WithUpstream, err error)
}

func (j JobService) Add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	me := errors.NewMultiError("add specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	jobs, err := j.generateJobs(ctx, jobTenant, validatedSpecs)
	me.Append(err)

	addedJobs, err := j.repo.Add(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, jobTenant.ProjectName(), addedJobs)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) Update(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	me := errors.NewMultiError("update specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	jobs, err := j.generateJobs(ctx, jobTenant, validatedSpecs)
	me.Append(err)

	updatedJobs, err := j.repo.Update(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, jobTenant.ProjectName(), updatedJobs)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag bool, forceFlag bool) (affectedDownstream []job.FullName, err error) {
	downstreamFullNames, err := j.repo.GetDownstreamFullNames(ctx, jobTenant.ProjectName(), jobName)
	if err != nil {
		return nil, err
	}

	if len(downstreamFullNames) > 0 && !forceFlag {
		errorMsg := fmt.Sprintf("job is being used by %s", downstreamFullNames)
		return nil, errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg)
	}

	return downstreamFullNames, j.repo.Delete(ctx, jobTenant.ProjectName(), jobName, cleanFlag)
}

func (j JobService) Get(ctx context.Context, filters ...filter.FilterOpt) (*job.Spec, error) {
	jobSpecs, err := j.GetAll(ctx, filters...)
	if err != nil {
		return nil, err
	}

	return jobSpecs[0], nil
}

func (j JobService) GetAll(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Spec, error) {
	f := filter.NewFilter(filters...)

	// when resource destination exist, filter by destination
	if f.Contains(filter.ResourceDestination) {
		return j.repo.GetAllByResourceDestination(ctx, f.GetValue(filter.ResourceDestination))
	}

	// when project name and job name exist, filter by project name and job name
	if f.Contains(filter.ProjectName, filter.JobName) {
		if jobSpec, err := j.repo.GetByJobName(ctx,
			f.GetValue(filter.ProjectName),
			f.GetValue(filter.JobName),
		); err != nil {
			return nil, err
		} else {
			return []*job.Spec{jobSpec}, nil
		}
	}

	// when project name exist, filter by project name
	if f.Contains(filter.ProjectName) {
		return j.repo.GetAllByProjectName(ctx, f.GetValue(filter.ProjectName))
	}

	return nil, nil
}

func (j JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	me := errors.NewMultiError("replace all specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	toAdd, toUpdate, toDelete, err := j.differentiateSpecs(ctx, jobTenant, validatedSpecs)
	me.Append(err)

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	me.Append(err)

	addedJobs, err := j.bulkAdd(ctx, tenantWithDetails, toAdd)
	me.Append(err)

	updatedJobs, err := j.bulkUpdate(ctx, tenantWithDetails, toUpdate)
	me.Append(err)

	err = j.bulkDelete(ctx, jobTenant, toDelete)
	me.Append(err)

	err = j.resolveAndSaveUpstreams(ctx, jobTenant.ProjectName(), addedJobs, updatedJobs)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) resolveAndSaveUpstreams(ctx context.Context, projectName tenant.ProjectName, jobsToResolve ...[]*job.Job) error {
	var allJobsToResolve []*job.Job
	for _, group := range jobsToResolve {
		allJobsToResolve = append(allJobsToResolve, group...)
	}
	if len(allJobsToResolve) == 0 {
		return nil
	}

	me := errors.NewMultiError("resolve and save upstream errors")
	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, projectName, allJobsToResolve)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(err)
}

func (j JobService) bulkAdd(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToAdd []*job.Spec) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk add specs errors")

	var jobsToAdd []*job.Job
	for _, spec := range specsToAdd {
		generatedJob, err := j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			me.Append(err)
			continue
		}
		jobsToAdd = append(jobsToAdd, generatedJob)
	}

	if len(jobsToAdd) == 0 {
		return nil, errors.MultiToError(me)
	}

	addedJobs, err := j.repo.Add(ctx, jobsToAdd)
	me.Append(err)

	return addedJobs, errors.MultiToError(me)
}

func (j JobService) bulkUpdate(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToUpdate []*job.Spec) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk add specs errors")

	var jobsToUpdate []*job.Job
	for _, spec := range specsToUpdate {
		generatedJob, err := j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			me.Append(err)
			continue
		}
		jobsToUpdate = append(jobsToUpdate, generatedJob)
	}

	if len(jobsToUpdate) == 0 {
		return nil, errors.MultiToError(me)
	}

	updatedJobs, err := j.repo.Update(ctx, jobsToUpdate)
	me.Append(err)

	return updatedJobs, errors.MultiToError(me)
}

func (j JobService) bulkDelete(ctx context.Context, jobTenant tenant.Tenant, toDelete []*job.Spec) error {
	me := errors.NewMultiError("bulk delete specs errors")

	for _, spec := range toDelete {
		downstreamFullNames, err := j.repo.GetDownstreamFullNames(ctx, jobTenant.ProjectName(), spec.Name())
		if err != nil {
			me.Append(err)
			continue
		}

		if len(downstreamFullNames) > 0 {
			errorMsg := fmt.Sprintf("job is being used by %s", downstreamFullNames)
			me.Append(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg))
			continue
		}

		err = j.repo.Delete(ctx, jobTenant.ProjectName(), spec.Name(), false)
		me.Append(err)
	}
	return errors.MultiToError(me)
}

func (j JobService) differentiateSpecs(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) (added []*job.Spec, modified []*job.Spec, deleted []*job.Spec, err error) {
	me := errors.NewMultiError("differentiate specs errors")

	existingJobSpecs, err := j.repo.GetAllSpecsByTenant(ctx, jobTenant)
	me.Append(err)

	var addedSpecs, modifiedSpecs, deletedSpecs []*job.Spec

	existingSpecsMap := job.Specs(existingJobSpecs).ToNameAndSpecMap()
	for _, incomingSpec := range specs {
		if spec, ok := existingSpecsMap[incomingSpec.Name()]; !ok {
			addedSpecs = append(addedSpecs, incomingSpec)
		} else {
			if !spec.IsEqual(incomingSpec) {
				modifiedSpecs = append(modifiedSpecs, incomingSpec)
			}
		}
	}

	incomingSpecsMap := job.Specs(specs).ToNameAndSpecMap()
	for _, existingJob := range existingJobSpecs {
		if _, ok := incomingSpecsMap[existingJob.Name()]; !ok {
			deletedSpecs = append(deletedSpecs, existingJob)
		}
	}
	return addedSpecs, modifiedSpecs, deletedSpecs, errors.MultiToError(me)
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
		generatedJob, err := j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			me.Append(err)
			continue
		}
		output = append(output, generatedJob)
	}
	return output, errors.MultiToError(me)
}

func (j JobService) generateJob(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) (*job.Job, error) {
	destination, err := j.pluginService.GenerateDestination(ctx, tenantWithDetails, spec.Task())
	if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
		errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	sources, err := j.pluginService.GenerateUpstreams(ctx, tenantWithDetails, spec, true)
	if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
		errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	return job.NewJob(tenantWithDetails.ToTenant(), spec, destination, sources), nil
}
