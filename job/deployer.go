package job

import (
	"context"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
)

type deployer struct {
	dependencyResolver        DependencyResolver
	priorityResolver          PriorityResolver
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory

	// scheduler for managing batch scheduled jobs
	batchScheduler models.SchedulerUnit
}

func NewDeployer(dependencyResolver DependencyResolver, priorityResolver PriorityResolver, projectJobSpecRepoFactory ProjectJobSpecRepoFactory, batchScheduler models.SchedulerUnit) *deployer {
	return &deployer{dependencyResolver: dependencyResolver, priorityResolver: priorityResolver, projectJobSpecRepoFactory: projectJobSpecRepoFactory, batchScheduler: batchScheduler}
}

func (d *deployer) Deploy(ctx context.Context, projectSpec models.ProjectSpec, progressObserver progress.Observer) error {
	// Get all jobs
	projectJobSpecRepo := d.projectJobSpecRepoFactory.New(projectSpec)
	jobSpecs, err := projectJobSpecRepo.GetAll(ctx)
	if err != nil {
		return err
	}
	d.notifyProgress(progressObserver, &models.ProgressJobSpecFetch{})

	// fetch job dependencies
	jobDependencies, err := d.dependencyResolver.FetchJobDependencies(ctx, projectSpec.ID)
	if err != nil {
		return err
	}
	d.notifyProgress(progressObserver, &models.ProgressJobSpecDependencyFetch{})

	// enrich with job dependencies
	jobSpecs, err = d.enrichJobSpecWithJobDependencies(ctx, jobSpecs, jobDependencies)
	if err != nil {
		return err
	}
	d.notifyProgress(progressObserver, &models.ProgressJobSpecJobDependencyEnrich{})

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
	for _, jobs := range jobSpecGroup {
		if len(jobs) == 0 {
			continue
		}
		if err := d.batchScheduler.DeployJobs(ctx, jobs[0].NamespaceSpec, jobs, progressObserver); err != nil {
			return err
		}
	}

	return nil
}

func (d *deployer) enrichJobSpecWithJobDependencies(ctx context.Context, jobSpecs []models.JobSpec, jobDependencies []models.JobIDDependenciesPair) ([]models.JobSpec, error) {
	// create jobSpecMap
	jobSpecMap, err := d.createJobSpecMap(ctx, jobSpecs, jobDependencies)
	if err != nil {
		return nil, err
	}

	// enrich jobs with dependencies
	var enrichedJobSpecs []models.JobSpec
	jobDependencyMap := models.JobIDDependenciesPairs(jobDependencies).GetJobDependencyMap()
	for _, jobSpec := range jobSpecs {
		if len(jobDependencyMap[jobSpec.ID]) > 0 {
			jobSpec.Dependencies = make(map[string]models.JobSpecDependency)
		}

		for _, dep := range jobDependencyMap[jobSpec.ID] {
			dependentJob := jobSpecMap[dep.DependentJobID]
			dependentProject := dep.DependentProject
			jobSpec.Dependencies[dependentJob.Name] = models.JobSpecDependency{
				Project: &dependentProject,
				Job:     &dependentJob,
				Type:    dep.Type,
			}
		}
		enrichedJobSpecs = append(enrichedJobSpecs, jobSpec)
	}
	return enrichedJobSpecs, nil
}

func (d *deployer) createJobSpecMap(ctx context.Context, jobSpecs []models.JobSpec, jobDependencies []models.JobIDDependenciesPair) (map[uuid.UUID]models.JobSpec, error) {
	jobSpecMap := make(map[uuid.UUID]models.JobSpec)
	externalProjectJobSpecs, err := d.getExternalProjectJobSpecs(ctx, jobDependencies)
	if err != nil {
		return nil, err
	}
	for _, jobSpec := range append(externalProjectJobSpecs, jobSpecs...) {
		jobSpecMap[jobSpec.ID] = jobSpec
	}
	return jobSpecMap, nil
}

func (d *deployer) getExternalProjectJobSpecs(ctx context.Context, jobDependencies []models.JobIDDependenciesPair) ([]models.JobSpec, error) {
	var externalJobSpecs []models.JobSpec
	for _, dependencies := range models.JobIDDependenciesPairs(jobDependencies).GetInterProjectDependencies() {
		var dependencyJobIDs []uuid.UUID
		for _, dependency := range dependencies {
			dependencyJobIDs = append(dependencyJobIDs, dependency.DependentJobID)
		}

		projectJobSpecRepo := d.projectJobSpecRepoFactory.New(dependencies[0].DependentProject)
		specs, err := projectJobSpecRepo.GetByIDs(ctx, dependencyJobIDs)
		if err != nil {
			return nil, err
		}

		externalJobSpecs = append(externalJobSpecs, specs...)
	}
	return externalJobSpecs, nil
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
