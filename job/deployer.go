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
	d.l.Debug("job dependency fetched", "request id", jobDeployment.ID.UUID(), "project name", jobDeployment.Project.Name)

	mappedProjectJobPairsByJobID, err := d.getMappedProjectJobPairsByJobID(ctx, jobDeployment.Project)
	if err != nil {
		return err
	}

	for _, jobSpec := range jobSpecs {
		if jobSpec.Dependencies == nil {
			jobSpec.Dependencies = make(map[string]models.JobSpecDependency)
		}
		projectJobPairs := mappedProjectJobPairsByJobID[jobSpec.ID]
		if err := d.enrichJobSpec(ctx, jobSpec, jobDeployment.Project, projectJobPairs); err != nil {
			return err
		}
	}

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

func (d *deployer) enrichJobSpec(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec, projectJobPairs []store.ProjectJobPair) error {
	if err := d.enrichWithStaticDependencies(ctx, jobSpec, projectSpec); err != nil {
		return fmt.Errorf("error while enriching jobspec %d with static dependencies: %w", jobSpec.ID, err)
	}
	d.enrichWithResourceDependencies(jobSpec, projectSpec, projectJobPairs)
	d.enrichWithHookDependencies(jobSpec)
	return nil
}

func (d *deployer) enrichWithStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec) error {
	projectJobSpecRepo := d.projectJobSpecRepoFactory.New(projectSpec)
	_, err := d.dependencyResolver.ResolveStaticDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	return err
}

func (*deployer) enrichWithResourceDependencies(jobSpec models.JobSpec, projectSpec models.ProjectSpec, projectJobPairs []store.ProjectJobPair) {
	for _, pair := range projectJobPairs {
		jobName := pair.Job.Name
		dependencyType := models.JobSpecDependencyTypeIntra
		if pair.Project.ID.UUID() != projectSpec.ID.UUID() {
			dependencyType = models.JobSpecDependencyTypeInter
		}
		project := &pair.Project
		job := &pair.Job
		jobSpec.Dependencies[jobName] = models.JobSpecDependency{
			Project: project,
			Job:     job,
			Type:    dependencyType,
		}
	}
}

func (d *deployer) enrichWithHookDependencies(jobSpec models.JobSpec) {
	hooks := d.dependencyResolver.FetchHookWithDependencies(jobSpec)
	jobSpec.Hooks = append(jobSpec.Hooks, hooks...)
}

func (d *deployer) getMappedProjectJobPairsByJobID(ctx context.Context, projectSpec models.ProjectSpec) (map[uuid.UUID][]store.ProjectJobPair, error) {
	jobSources, err := d.jobSourceRepository.GetAll(ctx, projectSpec.ID)
	if err != nil {
		return nil, fmt.Errorf("error getting job sources for project id %s: %w", projectSpec.ID.UUID(), err)
	}

	projectRepository := d.projectJobSpecRepoFactory.New(projectSpec)
	mappedProjectJobPairsByJobID := make(map[uuid.UUID][]store.ProjectJobPair)
	for _, source := range jobSources {
		jobPairs, err := projectRepository.GetByDestination(ctx, source.ResourceURN)
		if err != nil {
			if !errors.Is(err, store.ErrResourceNotFound) {
				return nil, fmt.Errorf("error getting dependency jobspec for job id %s: %w", source.JobID, err)
			}
		}
		mappedProjectJobPairsByJobID[source.JobID] = append(mappedProjectJobPairsByJobID[source.JobID], jobPairs...)
	}
	return mappedProjectJobPairsByJobID, nil
}
