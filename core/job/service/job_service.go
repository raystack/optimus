package service

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/scheduling"
	"github.com/odpf/optimus/internal/errors"
	"golang.org/x/net/context"
)

type JobService struct {
	repo           JobRepository
	dependencyRepo JobDependencyRepository

	pluginService      PluginService
	dependencyResolver DependencyResolver

	deployManager scheduling.DeploymentManager
}

type JobRepository interface {
	Save(ctx context.Context, jobs []*job.Job) error
	GetJobWithDependencies(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error)
}

type JobDependencyRepository interface {
	Save(ctx context.Context, jobsWithDependencies []*job.WithDependency) error
}

type DependencyResolver interface {
	Resolve(ctx context.Context, jobs []*job.Job) ([]*job.WithDependency, error)
}

func (j JobService) Add(ctx context.Context, jobSpecs []*dto.JobSpec) (scheduling.DeploymentID, error) {
	var jobs []*job.Job
	var jobErr error

	for _, spec := range jobSpecs {
		if err := spec.Validate(); err != nil {
			jobErr = multierror.Append(jobErr, errors.NewError(errors.ErrFailedPrecond, job.EntityJob, err.Error()))
			continue
		}

		destination, err := j.pluginService.GenerateDestination(ctx, spec.Task(), spec.Tenant())
		if err != nil {
			jobErr = multierror.Append(jobErr, errors.NewError(errors.ErrInternalError, job.EntityJob, err.Error()))
			continue
		}

		sources, err := j.pluginService.GenerateDependencies(ctx, spec, true)
		if err != nil {
			jobErr = multierror.Append(jobErr, errors.NewError(errors.ErrInternalError, job.EntityJob, err.Error()))
			continue
		}

		jobs = append(jobs, job.NewJob(spec, destination, sources))
	}

	if len(jobs) == 0 {
		return scheduling.DeploymentID{}, jobErr
	}

	if err := j.repo.Save(ctx, jobs); err != nil {
		return scheduling.DeploymentID{}, err
	}

	jobsWithDependencies, err := j.dependencyResolver.Resolve(ctx, jobs)
	if err != nil {
		return scheduling.DeploymentID{}, err
	}
	jobErr = multierror.Append(jobErr, j.getDependencyErrors(jobsWithDependencies))

	if err := j.dependencyRepo.Save(ctx, jobsWithDependencies); err != nil {
		return scheduling.DeploymentID{}, err
	}

	// send deployment request
	deploymentID, err := j.deployManager.Create(ctx, jobSpecs[0].Tenant().Project().Name())
	if err != nil {
		return scheduling.DeploymentID{}, err
	}

	return deploymentID, jobErr
}

func (j JobService) getDependencyErrors(jobsWithDependencies []*job.WithDependency) error {
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
