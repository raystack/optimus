package job

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
)

type deployer struct {
	l log.Logger

	dependencyResolver DependencyResolver
	priorityResolver   PriorityResolver
	namespaceService   service.NamespaceService

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit

	deployRepository store.JobDeploymentRepository
}

func NewDeployer(l log.Logger, dependencyResolver DependencyResolver, priorityResolver PriorityResolver, batchScheduler models.SchedulerUnit,
	deployRepository store.JobDeploymentRepository, namespaceService service.NamespaceService) *deployer {
	return &deployer{l: l, dependencyResolver: dependencyResolver, priorityResolver: priorityResolver, batchScheduler: batchScheduler,
		deployRepository: deployRepository, namespaceService: namespaceService}
}

func (d *deployer) Deploy(ctx context.Context, jobDeployment models.JobDeployment) (deployError error) {
	// fetch job specs and enrich with its dependencies
	jobSpecs, err := d.dependencyResolver.FetchJobSpecsWithJobDependencies(ctx, jobDeployment.Project)
	if err != nil {
		return err
	}
	d.l.Debug("job dependency fetched", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)

	// Get all job specs and enrich with hook dependencies
	jobSpecs = d.enrichJobSpecWithHookDependencies(jobSpecs)

	// Resolve priority
	jobSpecs, err = d.priorityResolver.Resolve(ctx, jobSpecs, nil)
	if err != nil {
		return err
	}
	d.l.Debug("job priority resolved", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)

	// Compile & Deploy
	jobSpecGroup := models.JobSpecs(jobSpecs).GroupJobsPerNamespace()
	for namespaceName, jobs := range jobSpecGroup {
		// fetch the namespace spec with secrets
		namespaceSpec, err := d.namespaceService.Get(ctx, jobDeployment.Project.Name, namespaceName)
		if err != nil {
			deployError = multierror.Append(deployError, err)
			continue
		}

		// deploy per namespace
		deployNamespaceDetail, err := d.batchScheduler.DeployJobsVerbose(ctx, namespaceSpec, jobs)
		if err != nil {
			deployError = multierror.Append(deployError, err)
			continue
		}
		jobDeployment.Details.Failures = append(jobDeployment.Details.Failures, deployNamespaceDetail.Failures...)
		jobDeployment.Details.FailureCount += deployNamespaceDetail.FailureCount
		jobDeployment.Details.SuccessCount += deployNamespaceDetail.SuccessCount

		// clean scheduler storage
		if err := d.cleanPerNamespace(ctx, namespaceSpec, jobs); err != nil {
			deployError = multierror.Append(deployError, err)
		}

		d.l.Debug(fmt.Sprintf("namespace %s deployed", namespaceName), "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)
	}

	if err := d.completeJobDeployment(ctx, jobDeployment); err != nil {
		return err
	}

	d.l.Info("job deployment finished", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)
	return deployError
}

func (d *deployer) completeJobDeployment(ctx context.Context, jobDeployment models.JobDeployment) error {
	if len(jobDeployment.Details.Failures) > 0 {
		jobDeployment.Status = models.JobDeploymentStatusFailed
	} else {
		jobDeployment.Status = models.JobDeploymentStatusSucceed
	}
	return d.deployRepository.Update(ctx, jobDeployment)
}

func (d *deployer) cleanPerNamespace(ctx context.Context, namespaceSpec models.NamespaceSpec, jobs []models.JobSpec) error {
	// get all stored job names
	schedulerJobs, err := d.batchScheduler.ListJobs(ctx, namespaceSpec, models.SchedulerListOptions{OnlyName: true})
	if err != nil {
		return err
	}
	var destJobNames []string
	for _, j := range schedulerJobs {
		destJobNames = append(destJobNames, j.Name)
	}

	// filter what we need to keep/delete
	var sourceJobNames []string
	for _, jobSpec := range jobs {
		sourceJobNames = append(sourceJobNames, jobSpec.Name)
	}
	jobsToDelete := setSubtract(destJobNames, sourceJobNames)
	jobsToDelete = jobDeletionFilter(jobsToDelete)
	if len(jobsToDelete) > 0 {
		if err := d.batchScheduler.DeleteJobs(ctx, namespaceSpec, jobsToDelete, nil); err != nil {
			return err
		}
	}
	return nil
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
