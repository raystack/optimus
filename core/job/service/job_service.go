package service

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobService struct {
	repo JobRepository

	pluginService      PluginService
	dependencyResolver DependencyResolver

	tenantDetailsGetter TenantDetailsGetter
}

func NewJobService(repo JobRepository, pluginService PluginService, dependencyResolver DependencyResolver, tenantDetailsGetter TenantDetailsGetter) *JobService {
	return &JobService{repo: repo, pluginService: pluginService, dependencyResolver: dependencyResolver, tenantDetailsGetter: tenantDetailsGetter}
}

type PluginService interface {
	GenerateDestination(context.Context, *tenant.WithDetails, *job.Task) (string, error)
	GenerateDependencies(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]string, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type JobRepository interface {
	Add(ctx context.Context, jobs []*job.Job) (savedJobs []*job.Job, jobErrors error, err error)
	GetJobNameWithInternalDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) (map[job.Name][]*job.Dependency, error)
	SaveDependency(ctx context.Context, jobsWithDependencies []*job.WithDependency) error
}

type DependencyResolver interface {
	Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job) (jobWithDependencies []*job.WithDependency, dependencyErrors error, err error)
}

func (j JobService) Add(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.Spec) (jobErrors error, err error) {
	// TODO: initialize jobs, with unknown state

	validatedJobs, jobErrors, err := j.validateSpecs(jobs)
	if err != nil {
		return jobErrors, err
	}

	addJobErrors, err := j.add(ctx, jobTenant, validatedJobs)
	if addJobErrors != nil {
		jobErrors = multierror.Append(jobErrors, addJobErrors)
	}
	return jobErrors, err
}

func (j JobService) add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) (jobErrors error, systemErr error) {
	jobs, jobErrors, err := j.createJobs(ctx, jobTenant, specs)
	if err != nil {
		return jobErrors, err
	}

	jobs, saveErrors, err := j.repo.Add(ctx, jobs)
	if saveErrors != nil {
		jobErrors = multierror.Append(jobErrors, saveErrors)
	}
	if err != nil {
		return jobErrors, err
	}

	jobsWithDependencies, dependencyErrors, err := j.dependencyResolver.Resolve(ctx, jobTenant.ProjectName(), jobs)
	if dependencyErrors != nil {
		jobErrors = multierror.Append(jobErrors, dependencyErrors)
	}
	if err != nil {
		return jobErrors, err
	}

	return jobErrors, j.repo.SaveDependency(ctx, jobsWithDependencies)
}

// TODO: instead of creating another list, lets just have a status in the spec that mark whether this job is skipped, or to_create
func (j JobService) validateSpecs(jobs []*job.Spec) (validatedJobs []*job.Spec, jobErrors error, err error) {
	for _, spec := range jobs {
		if err := spec.Validate(); err != nil {
			jobErrors = multierror.Append(jobErrors, err)
			continue
		}
		// TODO: mark job state
		validatedJobs = append(validatedJobs, spec)
	}

	// TODO: if we want to keep this, we need to check for the length of jobs
	if len(validatedJobs) == 0 {
		return nil, jobErrors, errors.NewError(errors.ErrInternalError, job.EntityJob, "all jobs failed the validation checks")
	}

	return validatedJobs, jobErrors, nil
}

func (j JobService) createJobs(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]*job.Job, error, error) {
	var jobs []*job.Job
	var jobErrors error

	detailedJobTenant, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		return nil, nil, err
	}

	for _, spec := range specs {
		destination, err := j.pluginService.GenerateDestination(ctx, detailedJobTenant, spec.Task())
		if err != nil && !errors.Is(err, ErrDependencyModNotFound) {
			errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
			jobErrors = multierror.Append(jobErrors, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
			continue
		}

		sources, err := j.pluginService.GenerateDependencies(ctx, detailedJobTenant, spec, true)
		if err != nil && !errors.Is(err, ErrDependencyModNotFound) {
			errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
			jobErrors = multierror.Append(jobErrors, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
			continue
		}

		jobs = append(jobs, job.NewJob(spec, destination, sources))
	}

	if len(jobs) == 0 {
		return nil, jobErrors, errors.NewError(errors.ErrInternalError, job.EntityJob, "no jobs to create")
	}

	return jobs, jobErrors, nil
}
