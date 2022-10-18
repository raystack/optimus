package service

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"

	"golang.org/x/net/context"
)

type JobService struct {
	repo          JobSpecRepository
	pluginService PluginService
}

type JobSpecRepository interface {
	Save(ctx context.Context, jobs []*job.Job) error
}

func (j JobService) Add(ctx context.Context, jobSpecs []*dto.JobSpec) error {
	// 1. validation
	// - if 1 job is failed to be validated/saved, should we fail them all? -> will proceed with what succeed, and notify the failed ones.
	// - job name validation

	var jobs []*job.Job
	for _, spec := range jobSpecs {
		// 2. identify job destination
		destination, err := j.pluginService.GenerateDestination(ctx, spec.Task(), spec.Tenant())
		if err != nil {
			return err
		}

		// 3. identify job sources
		sources, err := j.pluginService.GenerateDependencies(ctx, spec, true)
		if err != nil {
			return err
		}

		jobs = append(jobs, job.NewJob(spec, destination, sources))
	}

	// 4. persist to db
	if err := j.repo.Save(ctx, jobs); err != nil {
		return err
	}

	// 5. identify job dependency
	/*
		- from job source for inferred
		- from job spec for static
		- resolve external dependencies
	*/

	// 6. persist dependency
	/*
		storing the job dependency -> job_dependency: project, namespace, job name, optimus host
	*/

	// 7. send deployment request

	return nil
}
