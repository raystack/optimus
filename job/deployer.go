package job

import (
	"context"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

type deployer struct {
	dependencyResolver DependencyResolver
	priorityResolver   PriorityResolver
	namespaceService   service.NamespaceService

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit

	deployRepository store.JobDeploymentRepository
}

func NewDeployer(dependencyResolver DependencyResolver, priorityResolver PriorityResolver, batchScheduler models.SchedulerUnit,
	deployRepository store.JobDeploymentRepository, namespaceService service.NamespaceService) *deployer {
	return &deployer{dependencyResolver: dependencyResolver, priorityResolver: priorityResolver, batchScheduler: batchScheduler,
		deployRepository: deployRepository, namespaceService: namespaceService}
}

func (d *deployer) Deploy(ctx context.Context, jobDeployment models.JobDeployment) (deployError error) {
	jobDeployment.Status = models.JobDeploymentStatusInProgress
	if err := d.deployRepository.UpdateByID(ctx, jobDeployment); err != nil {
		return err
	}

	// fetch job specs and enrich with its dependencies
	jobSpecs, err := d.dependencyResolver.FetchJobSpecsWithJobDependencies(ctx, jobDeployment.Project)
	if err != nil {
		return err
	}

	// Get all job specs and enrich with hook dependencies
	jobSpecs = d.enrichJobSpecWithHookDependencies(jobSpecs)

	// Resolve priority
	jobSpecs, err = d.priorityResolver.Resolve(ctx, jobSpecs, nil)
	if err != nil {
		return err
	}

	// Compile & Deploy
	jobSpecGroup := models.JobSpecs(jobSpecs).GroupJobsPerNamespace()
	for namespaceName, jobs := range jobSpecGroup {
		deployNamespaceDetail, err := d.deployPerNamespace(ctx, jobDeployment.Project.Name, namespaceName, jobs)
		if err != nil {
			deployError = multierror.Append(deployError, err)
		}
		if len(deployNamespaceDetail.Failures) > 0 {
			jobDeployment.Details.Failures = append(jobDeployment.Details.Failures, deployNamespaceDetail.Failures...)
		}
		jobDeployment.Details.TotalSuccess += deployNamespaceDetail.TotalSuccess
	}

	time.Sleep(time.Second * 15)

	if err := d.completeJobDeployment(ctx, jobDeployment); err != nil {
		return err
	}

	return deployError
}

func (d *deployer) completeJobDeployment(ctx context.Context, jobDeployment models.JobDeployment) error {
	if len(jobDeployment.Details.Failures) > 0 {
		jobDeployment.Status = models.JobDeploymentStatusFailed
	} else {
		jobDeployment.Status = models.JobDeploymentStatusSucceed
	}
	return d.deployRepository.UpdateByID(ctx, jobDeployment)
}

func (d *deployer) deployPerNamespace(ctx context.Context, projectName string, namespaceName string, jobs []models.JobSpec) (models.JobDeploymentDetail, error) {
	// fetch the namespace spec with secrets
	namespaceSpec, err := d.namespaceService.Get(ctx, projectName, namespaceName)
	if err != nil {
		return models.JobDeploymentDetail{}, err
	}
	return d.batchScheduler.DeployJobsVerbose(ctx, namespaceSpec, jobs)
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
