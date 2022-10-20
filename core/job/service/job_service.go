package service

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/scheduling"
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

func (j JobService) Add(ctx context.Context, jobSpecs []*dto.JobSpec) error {
	var jobs []*job.Job
	for _, spec := range jobSpecs {
		if err := spec.Validate(); err != nil {
			return err
		}

		destination, err := j.pluginService.GenerateDestination(ctx, spec.Task(), spec.Tenant())
		if err != nil {
			return err
		}

		sources, err := j.pluginService.GenerateDependencies(ctx, spec, true)
		if err != nil {
			return err
		}

		jobs = append(jobs, job.NewJob(spec, destination, sources))
	}

	if err := j.repo.Save(ctx, jobs); err != nil {
		return err
	}

	jobsWithDependencies, err := j.dependencyResolver.Resolve(ctx, jobs)
	if err != nil {
		return err
	}

	if err := j.dependencyRepo.Save(ctx, jobsWithDependencies); err != nil {
		return err
	}

	// send deployment request
	deploymentID, err := j.deployManager.Create(ctx, jobSpecs[0].Tenant().Project().Name())
	if err != nil {
		return err
	}

	// include unresolved dependencies in the response

	return nil
}
