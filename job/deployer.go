package job

import (
	"context"
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

func (d *deployer) Deploy(ctx context.Context, jobDeployment models.JobDeployment) error {
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
	deployError := d.deployNamespaces(ctx, &jobDeployment, jobSpecs)

	if err := d.completeJobDeployment(ctx, jobDeployment); err != nil {
		return err
	}

	d.l.Info("job deployment finished", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)
	return deployError
}

func (d *deployer) deployNamespaces(ctx context.Context, jobDeployment *models.JobDeployment, jobSpecs []models.JobSpec) error {
	var deployError error
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
	jobIDInferredDependenciesMap, err := d.getJobIDInferredDependenciesMap(ctx, deploymentProjectSpec, jobSpecs)
	if err != nil {
		return err
	}

	for i, job := range jobSpecs {
		targetJobSpec := job
		if err := d.enrichWithStaticDependencies(ctx, &targetJobSpec, deploymentProjectSpec); err != nil {
			return fmt.Errorf("error while enriching jobspec %d with static dependencies: %w", targetJobSpec.ID, err)
		}

		inferredDependencies := jobIDInferredDependenciesMap[job.ID]
		d.enrichWithInferredDependencies(&targetJobSpec, deploymentProjectSpec, inferredDependencies)

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

func (*deployer) enrichWithInferredDependencies(
	jobSpec *models.JobSpec,
	deploymentProjectSpec models.ProjectSpec,
	resourceDependencies []models.JobSpec,
) {
	if jobSpec.Dependencies == nil {
		jobSpec.Dependencies = make(map[string]models.JobSpecDependency)
	}
	for _, dependency := range resourceDependencies {
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

func (d *deployer) getJobIDInferredDependenciesMap(ctx context.Context, deploymentProjectSpec models.ProjectSpec,
	jobSpecs []models.JobSpec) (map[uuid.UUID][]models.JobSpec, error) {
	jobSources, err := d.jobSourceRepository.GetAll(ctx, deploymentProjectSpec.ID)
	if err != nil {
		return nil, fmt.Errorf("error getting job sources for project id %s: %w", deploymentProjectSpec.ID.UUID(), err)
	}

	resourceURNJobMap := models.JobSpecs(jobSpecs).GroupJobsByDestination()
	externalJobSpecs, err := d.getExternalJobSpecs(ctx, deploymentProjectSpec, resourceURNJobMap, jobSources)
	if err != nil {
		return nil, err
	}
	for _, externalJob := range externalJobSpecs {
		job := externalJob
		resourceURNJobMap[externalJob.ResourceDestination] = &job
	}

	// preparing the job dependency by using the resource URN job map
	jobIDDependenciesMap := make(map[uuid.UUID][]models.JobSpec)
	for _, source := range jobSources {
		if _, ok := resourceURNJobMap[source.ResourceURN]; !ok {
			continue
		}
		jobIDDependenciesMap[source.JobID] = append(jobIDDependenciesMap[source.JobID], *resourceURNJobMap[source.ResourceURN])
	}
	return jobIDDependenciesMap, nil
}

func (d *deployer) getExternalJobSpecs(ctx context.Context, deploymentProjectSpec models.ProjectSpec, resourceURNJobMap map[string]*models.JobSpec, jobSources []models.JobSource) ([]models.JobSpec, error) {
	var externalSources []string
	for _, source := range jobSources {
		if _, ok := resourceURNJobMap[source.ResourceURN]; ok {
			continue
		}
		externalSources = append(externalSources, source.ResourceURN)
	}

	if len(externalSources) == 0 {
		return nil, nil
	}

	projectRepository := d.projectJobSpecRepoFactory.New(deploymentProjectSpec)
	externalJobSpecs, err := projectRepository.GetByDestinations(ctx, externalSources)
	if err != nil {
		return nil, fmt.Errorf("error getting job specs of external project sources: %w", err)
	}
	return externalJobSpecs, nil
}
