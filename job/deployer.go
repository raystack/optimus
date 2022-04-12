package job

import (
	"context"

	"github.com/hashicorp/go-multierror"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

type deployer struct {
	dependencyResolver DependencyResolver
	priorityResolver   PriorityResolver
	namespaceService   service.NamespaceService

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit
}

func NewDeployer(dependencyResolver DependencyResolver, priorityResolver PriorityResolver,
	batchScheduler models.SchedulerUnit, namespaceService service.NamespaceService) *deployer {
	return &deployer{dependencyResolver: dependencyResolver, priorityResolver: priorityResolver,
		batchScheduler: batchScheduler, namespaceService: namespaceService}
}

func (d *deployer) Deploy(ctx context.Context, projectSpec models.ProjectSpec, progressObserver progress.Observer) (deployError error) {
	// fetch job specs and enrich with its dependencies
	jobSpecs, err := d.dependencyResolver.FetchJobSpecsWithJobDependencies(ctx, projectSpec, progressObserver)
	if err != nil {
		return err
	}
	d.notifyProgress(progressObserver, &models.ProgressJobSpecWithDependencyFetch{})

	// Get all job specs and enrich with hook dependencies
	jobSpecs = d.enrichJobSpecWithHookDependencies(jobSpecs)
	d.notifyProgress(progressObserver, &models.ProgressJobSpecHookDependencyEnrich{})

	// Resolve priority
	jobSpecs, err = d.priorityResolver.Resolve(ctx, jobSpecs, progressObserver)
	if err != nil {
		return err
	}

	// Compile & Deploy
	jobSpecGroup := models.JobSpecs(jobSpecs).GroupJobsPerNamespace()
	for namespaceName, jobs := range jobSpecGroup {
		err = d.deployPerNamespace(ctx, projectSpec.Name, namespaceName, jobs, progressObserver)
		if err != nil {
			deployError = multierror.Append(deployError, err)
		}
	}

	return deployError
}

func (d *deployer) deployPerNamespace(ctx context.Context, projectName string,
	namespaceName string, jobs []models.JobSpec, progressObserver progress.Observer) error {
	// fetch the namespace spec with secrets
	namespaceSpec, err := d.namespaceService.Get(ctx, projectName, namespaceName)
	if err != nil {
		return err
	}
	return d.batchScheduler.DeployJobs(ctx, namespaceSpec, jobs, progressObserver)
}

func (d *deployer) enrichJobSpecWithHookDependencies(jobSpecs []models.JobSpec) []models.JobSpec {
	var enrichedJobSpecs []models.JobSpec
	for _, jobSpec := range jobSpecs {
		hooks := d.dependencyResolver.FetchHookWithDependencies(jobSpec)
		if len(hooks) > 0 {
			jobSpec.Hooks = hooks
		}
		enrichedJobSpecs = append(enrichedJobSpecs, jobSpec)
	}
	return enrichedJobSpecs
}

func (d *deployer) notifyProgress(observer progress.Observer, e progress.Event) {
	if observer == nil {
		return
	}
	observer.Notify(e)
}
