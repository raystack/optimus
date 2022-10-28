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
	repo           JobRepository
	dependencyRepo JobDependencyRepository

	pluginService      PluginService
	dependencyResolver DependencyResolver

	tenantDetailsGetter TenantDetailsGetter

	deployManager DeploymentManager
}

func NewJobService(repo JobRepository, dependencyRepo JobDependencyRepository, pluginService PluginService, dependencyResolver DependencyResolver, tenantDetailsGetter TenantDetailsGetter, deployManager DeploymentManager) *JobService {
	return &JobService{repo: repo, dependencyRepo: dependencyRepo, pluginService: pluginService, dependencyResolver: dependencyResolver, tenantDetailsGetter: tenantDetailsGetter, deployManager: deployManager}
}

type PluginService interface {
	GenerateDestination(context.Context, *tenant.WithDetails, *job.Task) (string, error)
	GenerateDependencies(ctx context.Context, jobTenant *tenant.WithDetails, jobSpec *job.JobSpec, dryRun bool) ([]string, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type JobRepository interface {
	Save(ctx context.Context, jobs []*job.Job) error
	GetJobWithDependencies(ctx context.Context, projectName tenant.ProjectName, jobNames []job.Name) ([]*job.WithDependency, error)
}

type JobDependencyRepository interface {
	Save(ctx context.Context, jobsWithDependencies []*job.WithDependency) error
}

type DependencyResolver interface {
	Resolve(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error)
}

type DeploymentManager interface {
	Create(ctx context.Context, projectName tenant.ProjectName) (uuid.UUID, error)
}

func (j JobService) Add(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.JobSpec) (deploymentID uuid.UUID, jobErr error, systemErr error) {
	jobs, jobErr := j.createJobs(ctx, jobTenant, jobSpecs)
	if len(jobs) == 0 {
		return uuid.Nil, jobErr, nil
	}

	if err := j.repo.Save(ctx, jobs); err != nil {
		return uuid.Nil, jobErr, err
	}

	dependencyErr, err := j.resolveDependency(ctx, jobs)
	if err != nil {
		return uuid.Nil, jobErr, err
	}
	if dependencyErr != nil {
		jobErr = multierror.Append(jobErr, dependencyErr)
	}

	deploymentID, err = j.deployManager.Create(ctx, jobTenant.ProjectName())
	if err != nil {
		return uuid.Nil, jobErr, err
	}

	return deploymentID, jobErr, nil
}

func (j JobService) Validate(ctx context.Context, jobs []*job.JobSpec) ([]*job.JobSpec, error) {
	var validatedSpecs []*job.JobSpec
	var multiErr error
	for _, spec := range jobs {
		if err := spec.Validate(); err != nil {
			multiErr = multierror.Append(multiErr, err)
			continue
		}
		validatedSpecs = append(validatedSpecs, spec)
	}
	return validatedSpecs, multiErr
}

func (j JobService) resolveDependency(ctx context.Context, jobs []*job.Job) (dependencyErr error, systemErr error) {
	jobsWithDependencies, err := j.dependencyResolver.Resolve(ctx, jobs)
	if err != nil {
		return nil, err
	}

	dependencyErr = j.getDependencyErrors(jobsWithDependencies)

	if err := j.dependencyRepo.Save(ctx, jobsWithDependencies); err != nil {
		return dependencyErr, err
	}

	return dependencyErr, nil
}

func (j JobService) createJobs(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.JobSpec) ([]*job.Job, error) {
	var jobs []*job.Job
	var jobErr error

	detailedJobTenant, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		return nil, err
	}

	for _, spec := range jobSpecs {
		destination, err := j.pluginService.GenerateDestination(ctx, detailedJobTenant, spec.Task())
		if err != nil && !errors.Is(err, ErrDependencyModNotFound) {
			errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
			jobErr = multierror.Append(jobErr, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
			continue
		}

		sources, err := j.pluginService.GenerateDependencies(ctx, detailedJobTenant, spec, true)
		if err != nil && !errors.Is(err, ErrDependencyModNotFound) {
			errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
			jobErr = multierror.Append(jobErr, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))
			continue
		}

		jobs = append(jobs, job.NewJob(spec, destination, sources))
	}

	return jobs, jobErr
}

func (JobService) getDependencyErrors(jobsWithDependencies []*job.WithDependency) error {
	var dependencyErr error
	for _, jobWithDependencies := range jobsWithDependencies {
		for _, unresolvedDependency := range jobWithDependencies.UnresolvedDependencies() {
			if unresolvedDependency.IsStaticDependency() {
				errMsg := fmt.Sprintf("[%s] error: %s unknown dependency", jobWithDependencies.Name().String(), unresolvedDependency.JobName)
				dependencyErr = multierror.Append(dependencyErr, errors.NewError(errors.ErrNotFound, job.EntityJob, errMsg))
			}
		}
	}
	return dependencyErr
}
