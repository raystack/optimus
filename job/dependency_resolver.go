package job

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/odpf/optimus/service"
	"strings"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/pkg/errors"
)

var (
	ErrUnknownLocalDependency        = errors.New("unknown local dependency")
	ErrUnknownCrossProjectDependency = errors.New("unknown cross project dependency")
	UnknownRuntimeDependencyMessage  = "could not find registered destination '%s' during compiling dependencies for the provided job '%s', " +
		"please check if the source is correct, " +
		"if it is and want this to be ignored as dependency, " +
		"check docs how this can be done in used transformation task"
)

const InterJobDependencyNameSections = 2

type dependencyResolver struct {
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory
	dependencyRepoFactory     DependencyRepoFactory
	projectService            service.ProjectService
}

// Resolve resolves all kind of dependencies (inter/intra project, static deps) of a given JobSpec
func (r *dependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec,
	observer progress.Observer) (models.JobSpec, error) {
	if ctx.Err() != nil {
		return models.JobSpec{}, ctx.Err()
	}

	projectJobSpecRepo := r.projectJobSpecRepoFactory.New(projectSpec)
	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := r.resolveInferredDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo, observer)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	jobSpec, err = r.resolveStaticDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve inter hook dependencies
	jobSpec = r.resolveHookDependencies(jobSpec)

	return jobSpec, nil
}

// ResolveAndPersist resolve inter/intra dependencies inferred by optimus and persist
func (r *dependencyResolver) ResolveAndPersist(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec,
	observer progress.Observer) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	projectJobSpecRepo := r.projectJobSpecRepoFactory.New(projectSpec)

	//resolve inter/intra dependencies inferred by optimus
	jobSpec, err := r.resolveInferredDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo, observer)
	if err != nil {
		return err
	}

	// resolve statically defined dependencies
	jobSpec, err = r.resolveStaticDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	if err != nil {
		return err
	}

	return r.persistDependencies(ctx, projectSpec, jobSpec)
}

func (r *dependencyResolver) persistDependencies(ctx context.Context, projectSpec models.ProjectSpec,
	jobSpec models.JobSpec) error {
	dependencyRepo := r.dependencyRepoFactory.New(projectSpec)

	// delete from dependency table
	if err := dependencyRepo.DeleteByJobID(ctx, jobSpec.ID); err != nil {
		return err
	}

	for _, dependency := range jobSpec.Dependencies {
		// insert the new ones
		jobDependency := store.JobDependency{
			JobID:              jobSpec.ID,
			ProjectID:          projectSpec.ID,
			DependentJobID:     dependency.Job.ID,
			DependentProjectID: dependency.Project.ID,
			Type:               dependency.Type.String(),
		}
		err := dependencyRepo.Save(ctx, jobDependency)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *dependencyResolver) Fetch(ctx context.Context, projectSpec models.ProjectSpec) (map[uuid.UUID][]models.JobSpecDependency, error) {
	dependencyRepo := r.dependencyRepoFactory.New(projectSpec)
	jobDependencies, err := dependencyRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	interDependenciesMap := getInterDependencies(jobDependencies)

	projects, err := r.prepareProjects(ctx, projectSpec, interDependenciesMap)
	if err != nil {
		return nil, err
	}

	jobSpecMap, err := r.prepareJobSpecMap(ctx, projectSpec, projects, interDependenciesMap)
	if err != nil {
		return nil, err
	}

	jobDependenciesMap := make(map[uuid.UUID][]models.JobSpecDependency)
	for _, dep := range jobDependencies {
		dependentJob := jobSpecMap[dep.DependentJobID]
		dependentProject := projects[dep.DependentProjectID]
		jobDependenciesMap[dep.JobID] = append(jobDependenciesMap[dep.JobID], models.JobSpecDependency{
			Project: &dependentProject,
			Job:     &dependentJob,
			Type:    models.JobSpecDependencyType(dep.Type),
		})
	}

	return jobDependenciesMap, nil
}

func (r *dependencyResolver) prepareJobSpecMap(ctx context.Context, projectSpec models.ProjectSpec,
	projects map[uuid.UUID]models.ProjectSpec, interDependenciesMap map[uuid.UUID][]store.JobDependency) (map[uuid.UUID]models.JobSpec, error) {

	projectJobSpecRepo := r.projectJobSpecRepoFactory.New(projectSpec)
	intraProjectJobSpecs, err := projectJobSpecRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	interProjectJobSpecs, err := r.prepareExternalJobs(ctx, projects, interDependenciesMap)
	if err != nil {
		return nil, err
	}

	jobSpecMap := make(map[uuid.UUID]models.JobSpec)
	for _, jobSpec := range intraProjectJobSpecs {
		jobSpecMap[jobSpec.ID] = jobSpec
	}
	for _, jobSpec := range interProjectJobSpecs {
		jobSpecMap[jobSpec.ID] = jobSpec
	}

	return jobSpecMap, nil
}

func getInterDependencies(jobDependencies []store.JobDependency) map[uuid.UUID][]store.JobDependency {
	interDependenciesMap := make(map[uuid.UUID][]store.JobDependency)
	for _, dep := range jobDependencies {
		if dep.Type == models.JobSpecDependencyTypeInter.String() {
			interDependenciesMap[dep.DependentProjectID] = append(interDependenciesMap[dep.DependentProjectID], dep)
		}
	}
	return interDependenciesMap
}

func (r *dependencyResolver) prepareProjects(ctx context.Context, projectSpec models.ProjectSpec,
	interDependenciesMap map[uuid.UUID][]store.JobDependency) (map[uuid.UUID]models.ProjectSpec, error) {

	projects, err := r.prepareExternalProjects(ctx, interDependenciesMap)
	if err != nil {
		return nil, err
	}

	// include requested project in the map
	projects[projectSpec.ID] = projectSpec

	return projects, nil
}

func (r *dependencyResolver) prepareExternalProjects(ctx context.Context,
	interDependenciesMap map[uuid.UUID][]store.JobDependency) (map[uuid.UUID]models.ProjectSpec, error) {

	projectSpecMap := make(map[uuid.UUID]models.ProjectSpec)
	for externalProjectID, _ := range interDependenciesMap {
		dependencyProjectSpec, err := r.projectService.GetByID(ctx, externalProjectID)
		if err != nil {
			return nil, err
		}
		projectSpecMap[dependencyProjectSpec.ID] = dependencyProjectSpec
	}
	return projectSpecMap, nil
}

func (r *dependencyResolver) prepareExternalJobs(ctx context.Context,
	externalProjects map[uuid.UUID]models.ProjectSpec,
	interDependenciesMap map[uuid.UUID][]store.JobDependency) (map[uuid.UUID]models.JobSpec, error) {

	externalJobMap := make(map[uuid.UUID]models.JobSpec)
	for externalProjectID, dependencies := range interDependenciesMap {
		projectSpec := externalProjects[externalProjectID]
		projectJobSpecRepo := r.projectJobSpecRepoFactory.New(projectSpec)

		var dependencyJobIDs []uuid.UUID
		for _, dependency := range dependencies {
			dependencyJobIDs = append(dependencyJobIDs, dependency.DependentJobID)
		}

		externalJobSpecs, err := projectJobSpecRepo.GetByIDs(ctx, dependencyJobIDs)
		if err != nil {
			return nil, err
		}

		for _, externalJob := range externalJobSpecs {
			externalJobMap[externalJob.ID] = externalJob
		}
	}
	return externalJobMap, nil
}

func (r *dependencyResolver) getDependencyMap(ctx context.Context, projectSpec models.ProjectSpec) (map[uuid.UUID][]store.JobDependency, error) {
	// get all dependencies per project
	dependencyRepo := r.dependencyRepoFactory.New(projectSpec)
	jobDependencies, err := dependencyRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	// create map of dependencies per job ID
	jobDependencyMap := make(map[uuid.UUID][]store.JobDependency)
	for _, dependency := range jobDependencies {
		jobDependencyMap[dependency.JobID] = append(jobDependencyMap[dependency.JobID], dependency)
	}
	return jobDependencyMap, nil
}

func (r *dependencyResolver) resolveInferredDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository, observer progress.Observer) (models.JobSpec, error) {
	// get destinations of dependencies, assets should be dependent on
	var jobDependencies []string
	if jobSpec.Task.Unit.DependencyMod != nil {
		resp, err := jobSpec.Task.Unit.DependencyMod.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
			Config:  models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			Project: projectSpec,
		})
		if err != nil {
			return models.JobSpec{}, err
		}
		jobDependencies = resp.Dependencies
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependencies {
		projectJobPairs, err := projectJobSpecRepo.GetByDestination(ctx, depDestination)
		if err != nil && err != store.ErrResourceNotFound {
			return jobSpec, fmt.Errorf("runtime dependency evaluation failed: %w", err)
		}
		if len(projectJobPairs) == 0 {
			// should not fail for unknown dependency, its okay to not have a upstream job
			// registered in optimus project and still refer to them in our job
			r.notifyProgress(observer, &EventJobSpecUnknownDependencyUsed{Job: jobSpec.Name, Dependency: depDestination})
			continue
		}
		dep := extractDependency(projectJobPairs, projectSpec)
		jobSpec.Dependencies[dep.Job.Name] = dep
	}

	return jobSpec, nil
}

// extractDependency extracts tries to find the upstream dependency from multiple matches
// type of dependency is decided based on if the job belongs to same project or other
// Note(kushsharma): correct way to do this is by creating a unique destination for each job
// this will require us to either change the database schema or add some kind of
// unique literal convention
func extractDependency(projectJobPairs []store.ProjectJobPair, projectSpec models.ProjectSpec) models.JobSpecDependency {
	var dep models.JobSpecDependency
	if len(projectJobPairs) == 1 {
		dep = models.JobSpecDependency{
			Job:     &projectJobPairs[0].Job,
			Project: &projectJobPairs[0].Project,
			Type:    models.JobSpecDependencyTypeIntra,
		}

		if projectJobPairs[0].Project.Name != projectSpec.Name {
			// if doesn't belong to same project, this is inter
			dep.Type = models.JobSpecDependencyTypeInter
		}
		return dep
	}

	// multiple projects found, this should not happen ideally and we should make
	// each destination unique, but now this has happened, give higher priority
	// to current project if found or choose any from the rest
	for _, pair := range projectJobPairs {
		if pair.Project.Name == projectSpec.Name {
			return models.JobSpecDependency{
				Job:     &pair.Job,
				Project: &pair.Project,
				Type:    models.JobSpecDependencyTypeIntra,
			}
		}
	}

	// dependency doesn't belong to this project, choose any(first)
	return models.JobSpecDependency{
		Job:     &projectJobPairs[0].Job,
		Project: &projectJobPairs[0].Project,
		Type:    models.JobSpecDependencyTypeInter,
	}
}

// update named (explicit/static) dependencies if unresolved with its spec model
// this can normally happen when reading specs from a store[local/postgres]
func (r *dependencyResolver) resolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				{
					job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
					if err != nil {
						return models.JobSpec{}, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, err)
					}
					depSpec.Job = &job
					depSpec.Project = &projectSpec
					jobSpec.Dependencies[depName] = depSpec
				}
			case models.JobSpecDependencyTypeInter:
				{
					// extract project name
					depParts := strings.SplitN(depName, "/", InterJobDependencyNameSections)
					if len(depParts) != InterJobDependencyNameSections {
						return models.JobSpec{}, fmt.Errorf("%s dependency should be in 'project_name/job_name' format: %s", models.JobSpecDependencyTypeInter, depName)
					}
					projectName := depParts[0]
					jobName := depParts[1]
					job, proj, err := projectJobSpecRepo.GetByNameForProject(ctx, projectName, jobName)
					if err != nil {
						return models.JobSpec{}, fmt.Errorf("%s for job %s: %w", ErrUnknownCrossProjectDependency, depName, err)
					}
					depSpec.Job = &job
					depSpec.Project = &proj
					jobSpec.Dependencies[depName] = depSpec
				}
			default:
				return models.JobSpec{}, fmt.Errorf("unsupported dependency type: %s", depSpec.Type)
			}
		}
	}

	return jobSpec, nil
}

// hooks can be dependent on each other inside a job spec, this will populate
// the local array that points to its dependent hook
func (r *dependencyResolver) resolveHookDependencies(jobSpec models.JobSpec) models.JobSpec {
	for hookIdx, jobHook := range jobSpec.Hooks {
		jobHook.DependsOn = nil
		for _, depends := range jobHook.Unit.Info().DependsOn {
			dependentHook, err := jobSpec.GetHookByName(depends)
			if err == nil {
				jobHook.DependsOn = append(jobHook.DependsOn, &dependentHook)
			}
		}
		jobSpec.Hooks[hookIdx] = jobHook
	}
	return jobSpec
}

//func (r *dependencyResolver) ResolveHookDependencies(jobSpec models.JobSpec) (map[string][]models.JobSpecHook, error) {
//	jobSpecHooks := make(map[string][]models.JobSpecHook)
//	for hookIdx, jobHook := range jobSpec.Hooks {
//		var jobHooks []*models.JobSpecHook
//		for _, depends := range jobHook.Unit.Info().DependsOn {
//			dependentHook, err := jobSpec.GetHookByName(depends)
//			if err == nil {
//				jobHooks = append(jobHooks, &dependentHook)
//			}
//		}
//		jobSpecHooks[hookIdx] = jobHook
//	}
//	return jobSpec, nil
//}

func (r *dependencyResolver) notifyProgress(observer progress.Observer, e progress.Event) {
	if observer == nil {
		return
	}
	observer.Notify(e)
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver(projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	dependencyRepoFactory DependencyRepoFactory,
	projectService service.ProjectService) *dependencyResolver {
	return &dependencyResolver{
		projectJobSpecRepoFactory: projectJobSpecRepoFactory,
		dependencyRepoFactory:     dependencyRepoFactory,
		projectService:            projectService,
	}
}
