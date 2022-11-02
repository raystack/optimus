package service

import (
	"fmt"

	"github.com/google/uuid"
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

	deployManager DeploymentManager
}

func NewJobService(repo JobRepository, pluginService PluginService, dependencyResolver DependencyResolver, tenantDetailsGetter TenantDetailsGetter, deployManager DeploymentManager) *JobService {
	return &JobService{repo: repo, pluginService: pluginService, dependencyResolver: dependencyResolver, tenantDetailsGetter: tenantDetailsGetter, deployManager: deployManager}
}

type PluginService interface {
	GenerateDestination(context.Context, *tenant.WithDetails, *job.Task) (string, error)
	GenerateDependencies(ctx context.Context, jobTenant *tenant.WithDetails, jobSpec *job.JobSpec, dryRun bool) ([]string, error)
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

type DeploymentManager interface {
	Create(ctx context.Context, projectName tenant.ProjectName) (uuid.UUID, error)
}

func (j JobService) AddAndDeploy(ctx context.Context, jobTenant tenant.Tenant, jobs []*job.JobSpec) (deploymentID uuid.UUID, jobErrors error, err error) {
	validatedJobs, jobErrors, err := j.validateSpecs(jobs)
	if err != nil {
		return uuid.Nil, jobErrors, err
	}

	addJobErrors, err := j.add(ctx, jobTenant, validatedJobs)
	if addJobErrors != nil {
		jobErrors = multierror.Append(jobErrors, addJobErrors)
	}
	if err != nil {
		return uuid.Nil, jobErrors, err
	}

	deploymentID, err = j.deployManager.Create(ctx, jobTenant.ProjectName())
	return deploymentID, jobErrors, err
}

func (j JobService) add(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.JobSpec) (jobErrors error, systemErr error) {
	jobs, jobErrors, err := j.createJobs(ctx, jobTenant, jobSpecs)
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

func (j JobService) validateSpecs(jobs []*job.JobSpec) (validatedJobs []*job.JobSpec, jobErrors error, err error) {
	for _, spec := range jobs {
		if err := spec.Validate(); err != nil {
			jobErrors = multierror.Append(jobErrors, err)
			continue
		}
		validatedJobs = append(validatedJobs, spec)
	}

	if len(validatedJobs) == 0 {
		return nil, jobErrors, errors.NewError(errors.ErrInternalError, job.EntityJob, "all jobs failed the validation checks")
	}

	return validatedJobs, jobErrors, nil
}

func (j JobService) createJobs(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.JobSpec) ([]*job.Job, error, error) {
	var jobs []*job.Job
	var jobErrors error

	detailedJobTenant, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		return nil, nil, err
	}

	for _, spec := range jobSpecs {
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
