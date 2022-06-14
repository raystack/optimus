package job

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
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

	deployRepository          store.JobDeploymentRepository
	jobSourceRepository       store.JobSourceRepository
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
}

func NewDeployer(
	l log.Logger,
	dependencyResolver DependencyResolver,
	priorityResolver PriorityResolver,
	namespaceService service.NamespaceService,
	deployRepository store.JobDeploymentRepository,
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	jobSourceRepository store.JobSourceRepository,
	batchScheduler models.SchedulerUnit,
) Deployer {
	return &deployer{
		l:                         l,
		dependencyResolver:        dependencyResolver,
		priorityResolver:          priorityResolver,
		batchScheduler:            batchScheduler,
		deployRepository:          deployRepository,
		namespaceService:          namespaceService,
		jobSourceRepository:       jobSourceRepository,
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
	}
}

func (d *deployer) Deploy(ctx context.Context, jobDeployment models.JobDeployment) (deployError error) {
	projectJobSpecRepo := d.projectJobSpecRepoFactory.New(jobDeployment.Project)
	jobSpecs, err := projectJobSpecRepo.GetAll(ctx)
	if err != nil {
		return err
	}
	d.l.Debug("job specs fetched", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)

	if err := d.enrichJobSpecs(ctx, jobSpecs, jobDeployment.Project); err != nil {
		return err
	}
	d.l.Debug("job specs enriched", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)

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

func (d *deployer) enrichJobSpecs(ctx context.Context, jobSpecs []models.JobSpec, deploymentProjectSpec models.ProjectSpec) error {
	jobsByDestination := models.JobSpecs(jobSpecs).GroupJobsByDestination()

	jobIDDependenciesMap, err := d.getJobIDDependenciesMap(ctx, deploymentProjectSpec, jobsByDestination)
	if err != nil {
		return err
	}

	for i, job := range jobSpecs {
		targetJobSpec := job
		if err := d.enrichWithStaticDependencies(ctx, &targetJobSpec, deploymentProjectSpec); err != nil {
			return fmt.Errorf("error while enriching jobspec %d with static dependencies: %w", targetJobSpec.ID, err)
		}

		dependencies := jobIDDependenciesMap[job.ID]
		d.enrichWithResourceDependencies(&targetJobSpec, deploymentProjectSpec, dependencies)

		d.enrichWithHookDependencies(&targetJobSpec)

		jobSpecs[i] = targetJobSpec
	}
	return nil
}

func (d *deployer) enrichWithStaticDependencies(
	ctx context.Context,
	jobSpec *models.JobSpec,
	deploymentProjectSpec models.ProjectSpec,
) error {
	projectJobSpecRepo := d.projectJobSpecRepoFactory.New(deploymentProjectSpec)
	staticDependencies, err := d.dependencyResolver.ResolveStaticDependencies(ctx, *jobSpec, deploymentProjectSpec, projectJobSpecRepo)
	jobSpec.Dependencies = staticDependencies
	return err
}

func (*deployer) enrichWithResourceDependencies(
	jobSpec *models.JobSpec,
	deploymentProjectSpec models.ProjectSpec,
	jobDependencies []models.JobSpec,
) {
	for _, dependency := range jobDependencies {
		dependencyType := models.JobSpecDependencyTypeIntra
		if dependency.NamespaceSpec.ProjectSpec.ID.UUID() != deploymentProjectSpec.ID.UUID() {
			dependencyType = models.JobSpecDependencyTypeInter
		}
		jobDependencySpec := dependency
		jobSpec.Dependencies[dependency.Name] = models.JobSpecDependency{
			Project: &jobDependencySpec.NamespaceSpec.ProjectSpec,
			Job:     &jobDependencySpec,
			Type:    dependencyType,
		}
	}
}

func (d *deployer) enrichWithHookDependencies(jobSpec *models.JobSpec) {
	hooks := d.dependencyResolver.FetchHookWithDependencies(*jobSpec)
	jobSpec.Hooks = hooks
}

func (d *deployer) getJobIDDependenciesMap(ctx context.Context, deploymentProjectSpec models.ProjectSpec,
	jobsByDestination map[string]models.JobSpec) (map[uuid.UUID][]models.JobSpec, error) {
	jobSources, err := d.jobSourceRepository.GetAll(ctx, deploymentProjectSpec.ID)
	if err != nil {
		return nil, fmt.Errorf("error getting job sources for project id %s: %w", deploymentProjectSpec.ID.UUID(), err)
	}

	// populating the resource URN job map
	resourceURNJobMap := make(map[string]models.JobSpec)
	for urn, job := range jobsByDestination {
		resourceURNJobMap[urn] = job
	}
	projectRepository := d.projectJobSpecRepoFactory.New(deploymentProjectSpec)
	for _, source := range jobSources {
		if _, ok := resourceURNJobMap[source.ResourceURN]; ok {
			continue
		}
		jobSpec, err := projectRepository.GetByDestination(ctx, source.ResourceURN)
		if err != nil {
			if !errors.Is(err, store.ErrResourceNotFound) {
				return nil, fmt.Errorf("error getting dependency jobspec for job id %s: %w", source.JobID, err)
			}
		}
		resourceURNJobMap[source.ResourceURN] = jobSpec
	}

	// preparing the job dependency by using the resource URN job map
	jobIDDependenciesMap := make(map[uuid.UUID][]models.JobSpec)
	for _, source := range jobSources {
		jobIDDependenciesMap[source.JobID] = append(jobIDDependenciesMap[source.JobID], resourceURNJobMap[source.ResourceURN])
	}
	return jobIDDependenciesMap, nil
}
