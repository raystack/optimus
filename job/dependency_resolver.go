package job

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store"
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
	jobSpecRepo   store.JobSpecRepository
	jobSourceRepo store.JobSourceRepository
	pluginService service.PluginService

	// TODO: will be deprecated along with Resolve method deprecation
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory

	externalDependencyResolver ExternalDependencyResolver
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver(
	jobSpecRepo store.JobSpecRepository,
	jobSourceRepo store.JobSourceRepository,
	pluginService service.PluginService,
	projectJobSpecRepoFactory ProjectJobSpecRepoFactory,
	externalDependencyResolver ExternalDependencyResolver,
) DependencyResolver {
	return &dependencyResolver{
		jobSpecRepo:                jobSpecRepo,
		jobSourceRepo:              jobSourceRepo,
		pluginService:              pluginService,
		projectJobSpecRepoFactory:  projectJobSpecRepoFactory,
		externalDependencyResolver: externalDependencyResolver,
	}
}

// Resolve resolves all kind of dependencies (inter/intra project, static deps) of a given JobSpec
// TODO: this method will be deprecated
func (d *dependencyResolver) Resolve(ctx context.Context, projectSpec models.ProjectSpec, jobSpec models.JobSpec,
	observer progress.Observer) (models.JobSpec, error) {
	if ctx.Err() != nil {
		return models.JobSpec{}, ctx.Err()
	}

	projectJobSpecRepo := d.projectJobSpecRepoFactory.New(projectSpec)
	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := d.resolveInferredDependencies(ctx, jobSpec, projectSpec, observer)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	jobSpec, err = d.resolveStaticDependencies(ctx, jobSpec, projectSpec, projectJobSpecRepo)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve inter hook dependencies
	jobSpec = d.resolveHookDependencies(jobSpec)

	return jobSpec, nil
}

// ResolveStaticDependencies return named (explicit/static) dependencies that unresolved with its spec model
// this is normally happen when reading specs from a store[local/postgres]
// unresolved dependencies will no longer exist in the map
// TODO: if we have field `projectJobFactory`, we might not need the `projectJobSpecRepository` parameter
func (*dependencyResolver) ResolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (map[string]models.JobSpecDependency, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if reflect.ValueOf(jobSpec).IsZero() {
		return nil, errors.New("job spec is empty")
	}
	if reflect.ValueOf(projectSpec).IsZero() {
		return nil, errors.New("project spec is empty")
	}
	if projectJobSpecRepo == nil {
		return nil, errors.New("project job spec repo is nil")
	}

	resolvedJobSpecDependencies := make(map[string]models.JobSpecDependency)
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
				if err != nil {
					return nil, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &projectSpec
				resolvedJobSpecDependencies[depName] = depSpec
			case models.JobSpecDependencyTypeInter:
				// extract project name
				depParts := strings.SplitN(depName, "/", InterJobDependencyNameSections)
				if len(depParts) != InterJobDependencyNameSections {
					return nil, fmt.Errorf("%s dependency should be in 'project_name/job_name' format: %s", models.JobSpecDependencyTypeInter, depName)
				}
				projectName := depParts[0]
				jobName := depParts[1]
				job, proj, err := projectJobSpecRepo.GetByNameForProject(ctx, projectName, jobName)
				if err != nil {
					return nil, fmt.Errorf("%s for job %s: %w", ErrUnknownCrossProjectDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &proj
				resolvedJobSpecDependencies[depName] = depSpec
			default:
				return nil, fmt.Errorf("unsupported dependency type: %s", depSpec.Type)
			}
		}
	}
	return resolvedJobSpecDependencies, nil
}

// TODO: this method will be deprecated (should be refactored to separate responsibility)
func (d *dependencyResolver) resolveInferredDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	observer progress.Observer) (models.JobSpec, error) {
	// get destinations of dependencies, assets should be dependent on
	namespace := jobSpec.NamespaceSpec
	namespace.ProjectSpec = projectSpec // TODO: Temp fix to to get secrets from project
	resp, err := d.pluginService.GenerateDependencies(ctx, jobSpec, namespace, false)
	if err != nil {
		if !errors.Is(err, service.ErrDependencyModNotFound) {
			return models.JobSpec{}, err
		}
	}
	if resp == nil || len(resp.Dependencies) == 0 {
		return jobSpec, nil
	}

	jobDependencies := resp.Dependencies
	if err := d.jobSourceRepo.Save(ctx, projectSpec.ID, jobSpec.ID, jobDependencies); err != nil {
		return models.JobSpec{}, fmt.Errorf("error persisting job sources for job %s: %w", jobSpec.Name, err)
	}

	// get job spec of these destinations and append to current jobSpec
	for _, depDestination := range jobDependencies {
		dependencyJobSpec, err := d.jobSpecRepo.GetJobByResourceDestination(ctx, depDestination)
		if err != nil {
			if errors.Is(err, store.ErrResourceNotFound) {
				// should not fail for unknown dependency, its okay to not have a upstream job
				// registered in optimus project and still refer to them in our job
				d.notifyProgress(observer, &models.ProgressJobSpecUnknownDependencyUsed{Job: jobSpec.Name, Dependency: depDestination})
				continue
			}
			return jobSpec, fmt.Errorf("runtime dependency evaluation failed: %w", err)
		}
		dep := extractDependency(dependencyJobSpec, projectSpec)
		jobSpec.Dependencies[dep.Job.Name] = dep
	}

	return jobSpec, nil
}

// extractDependency extracts tries to find the upstream dependency from multiple matches
// type of dependency is decided based on if the job belongs to same project or other
// Note(kushsharma): correct way to do this is by creating a unique destination for each job
// this will require us to either change the database schema or add some kind of
// unique literal convention
func extractDependency(dependencyJobSpec models.JobSpec, projectSpec models.ProjectSpec) models.JobSpecDependency {
	dep := models.JobSpecDependency{
		Job:     &dependencyJobSpec,
		Project: &dependencyJobSpec.NamespaceSpec.ProjectSpec,
		Type:    models.JobSpecDependencyTypeIntra,
	}

	if dep.Project.Name != projectSpec.Name {
		// if doesn't belong to same project, this is inter
		dep.Type = models.JobSpecDependencyTypeInter
	}
	return dep
}

// TODO: this method will be deprecated and replaced with ResolveStaticDependencies
func (*dependencyResolver) resolveStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec,
	projectJobSpecRepo store.ProjectJobSpecRepository) (models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for depName, depSpec := range jobSpec.Dependencies {
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				job, _, err := projectJobSpecRepo.GetByName(ctx, depName)
				if err != nil {
					return models.JobSpec{}, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, err)
				}
				depSpec.Job = &job
				depSpec.Project = &projectSpec
				jobSpec.Dependencies[depName] = depSpec
			case models.JobSpecDependencyTypeInter:
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
			default:
				return models.JobSpec{}, fmt.Errorf("unsupported dependency type: %s", depSpec.Type)
			}
		}
	}
	return jobSpec, nil
}

// hooks can be dependent on each other inside a job spec, this will populate
// the local array that points to its dependent hook
func (*dependencyResolver) resolveHookDependencies(jobSpec models.JobSpec) models.JobSpec {
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

func (d *dependencyResolver) GetJobSpecsWithDependencies(ctx context.Context, projectID models.ProjectID) ([]models.JobSpec, []models.UnknownDependency, error) {
	if ctx == nil {
		return nil, nil, errors.New("context is nil")
	}
	jobSpecs, err := d.jobSpecRepo.GetAllByProjectID(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}

	internalDependenciesByJobID, err := d.getInternalDependenciesByJobID(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}
	externalDependenciesByJobName, unknownDependencies, err := d.getExternalDependenciesByJobName(ctx, projectID, jobSpecs, internalDependenciesByJobID)
	if err != nil {
		return nil, nil, err
	}

	for i := 0; i < len(jobSpecs); i++ {
		internalDependencies := internalDependenciesByJobID[jobSpecs[i].ID]
		externalDependency := externalDependenciesByJobName[jobSpecs[i].Name]

		jobSpecs[i].Dependencies = d.groupDependencies(internalDependencies)
		jobSpecs[i].ExternalDependencies.OptimusDependencies = append(jobSpecs[i].ExternalDependencies.OptimusDependencies, externalDependency.OptimusDependencies...)
		jobSpecs[i].Hooks = d.fetchHookWithDependencies(jobSpecs[i])
	}
	return jobSpecs, unknownDependencies, nil
}

func (d *dependencyResolver) getInternalDependenciesByJobID(ctx context.Context, projectID models.ProjectID) (map[uuid.UUID][]models.JobSpec, error) {
	mergeInternalDependencies := func(input1, input2 map[uuid.UUID][]models.JobSpec) map[uuid.UUID][]models.JobSpec {
		for jobID, internalDependencies := range input2 {
			input1[jobID] = append(input1[jobID], internalDependencies...)
		}
		return input1
	}

	staticDependenciesPerJobID, err := d.jobSpecRepo.GetStaticDependenciesPerJobID(ctx, projectID)
	if err != nil {
		return nil, err
	}
	inferredDependenciesPerJobID, err := d.jobSpecRepo.GetInferredDependenciesPerJobID(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return mergeInternalDependencies(staticDependenciesPerJobID, inferredDependenciesPerJobID), nil
}

func (d *dependencyResolver) getExternalDependenciesByJobName(ctx context.Context, projectID models.ProjectID, jobSpecs []models.JobSpec, internalJobDependencies map[uuid.UUID][]models.JobSpec) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	mergeExternalDependencies := func(input1, input2 map[string]models.ExternalDependency) map[string]models.ExternalDependency {
		for jobName, externalDependency := range input2 {
			externalOptimusDependencies := input1[jobName].OptimusDependencies
			externalOptimusDependencies = append(externalOptimusDependencies, externalDependency.OptimusDependencies...)
			input1[jobName] = models.ExternalDependency{
				HTTPDependencies:    input1[jobName].HTTPDependencies,
				OptimusDependencies: externalOptimusDependencies,
			}
		}
		return input1
	}
	mergeUnknownDependencies := func(input1, input2 []models.UnknownDependency) []models.UnknownDependency {
		input1 = append(input1, input2...)
		return input1
	}

	unresolvedStaticDependenciesPerJobName, unknownDependenciesFromInternal := d.getUnresolvedStaticDependencies(jobSpecs, internalJobDependencies)
	staticExternalDependencyPerJobName, unknownDependenciesFromExternal, err := d.externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, unresolvedStaticDependenciesPerJobName)
	if err != nil {
		return nil, nil, err
	}
	unknownDependencies := mergeUnknownDependencies(unknownDependenciesFromInternal, unknownDependenciesFromExternal)

	unresolvedInferredDependenciesPerJobName, err := d.getUnresolvedInferredDependencies(ctx, projectID, jobSpecs, internalJobDependencies)
	if err != nil {
		return nil, nil, err
	}
	inferredExternalDependencyPerJobName, err := d.externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, unresolvedInferredDependenciesPerJobName)
	if err != nil {
		return nil, nil, err
	}

	externalDependenciesByJobName := mergeExternalDependencies(staticExternalDependencyPerJobName, inferredExternalDependencyPerJobName)
	return externalDependenciesByJobName, unknownDependencies, nil
}

func (d *dependencyResolver) getUnresolvedStaticDependencies(jobSpecs []models.JobSpec, internalJobDependencies map[uuid.UUID][]models.JobSpec) (map[string][]models.UnresolvedJobDependency, []models.UnknownDependency) {
	convertToUnknownDependencies := func(unknownStaticDependencyNames []string, jobName, projectName string) []models.UnknownDependency {
		unknownDependencies := make([]models.UnknownDependency, len(unknownStaticDependencyNames))
		for i := 0; i < len(unknownStaticDependencyNames); i++ {
			unknownDependencies[i] = models.UnknownDependency{
				JobName:               jobName,
				DependencyProjectName: projectName,
				DependencyJobName:     unknownStaticDependencyNames[i],
			}
		}
		return unknownDependencies
	}

	unresolvedStaticDependenciesPerJobName := make(map[string][]models.UnresolvedJobDependency)
	var unknownDependencies []models.UnknownDependency
	for _, jobSpec := range jobSpecs {
		resolvedDependencies := internalJobDependencies[jobSpec.ID]
		unresolvedStaticDependencies, unknownStaticDependencyNames := d.identifyUnresolvedStaticDependencies(jobSpec.Dependencies, resolvedDependencies)

		unknownDependenciesPerJob := convertToUnknownDependencies(unknownStaticDependencyNames, jobSpec.Name, jobSpec.GetProjectSpec().Name)
		unknownDependencies = append(unknownDependencies, unknownDependenciesPerJob...)

		if len(unresolvedStaticDependencies) > 0 {
			unresolvedStaticDependenciesPerJobName[jobSpec.Name] = unresolvedStaticDependencies
		}
	}
	return unresolvedStaticDependenciesPerJobName, unknownDependencies
}

func (*dependencyResolver) identifyUnresolvedStaticDependencies(jobDependencies map[string]models.JobSpecDependency, resolvedStaticDependencies []models.JobSpec) ([]models.UnresolvedJobDependency, []string) {
	var unresolvedStaticDependencies []models.UnresolvedJobDependency
	var unknownStaticDependencyNames []string
	for dependencyName := range jobDependencies {
		isResolved := false
		for _, resolvedStaticDependency := range resolvedStaticDependencies {
			if dependencyName == resolvedStaticDependency.Name || dependencyName == resolvedStaticDependency.GetFullName() {
				isResolved = true
				break
			}
		}
		if !isResolved {
			unresolvedDependency, isCrossProjectDependency := convertDependencyNamesToUnresolvedJobDependency(dependencyName)
			if !isCrossProjectDependency {
				unknownStaticDependencyNames = append(unknownStaticDependencyNames, dependencyName)
				continue
			}
			unresolvedStaticDependencies = append(unresolvedStaticDependencies, unresolvedDependency)
		}
	}
	return unresolvedStaticDependencies, unknownStaticDependencyNames
}

func convertDependencyNamesToUnresolvedJobDependency(dependencyName string) (models.UnresolvedJobDependency, bool) {
	splitName := strings.Split(dependencyName, "/")
	if expectedSplitLen := 2; len(splitName) != expectedSplitLen {
		return models.UnresolvedJobDependency{}, false
	}
	return models.UnresolvedJobDependency{
		ProjectName: splitName[0],
		JobName:     splitName[1],
	}, true
}

func (d *dependencyResolver) getUnresolvedInferredDependencies(ctx context.Context, projectID models.ProjectID,
	jobSpecs []models.JobSpec, internalJobDependencies map[uuid.UUID][]models.JobSpec) (map[string][]models.UnresolvedJobDependency, error) {
	inferredDependencyURNsPerJobID, err := d.jobSourceRepo.GetResourceURNsPerJobID(ctx, projectID)
	if err != nil {
		return nil, err
	}

	unresolvedInferredDependenciesPerJobName := make(map[string][]models.UnresolvedJobDependency)
	for _, jobSpec := range jobSpecs {
		resolvedDependencies := internalJobDependencies[jobSpec.ID]

		inferredDependencyURNs := inferredDependencyURNsPerJobID[jobSpec.ID]
		unresolvedInferredDependencies := d.identifyUnresolvedInferredDependencies(inferredDependencyURNs, resolvedDependencies)
		if len(unresolvedInferredDependencies) > 0 {
			unresolvedInferredDependenciesPerJobName[jobSpec.Name] = unresolvedInferredDependencies
		}
	}
	return unresolvedInferredDependenciesPerJobName, nil
}

func (*dependencyResolver) identifyUnresolvedInferredDependencies(inferredDependencies []string, resolvedDependencies []models.JobSpec) []models.UnresolvedJobDependency {
	var unresolvedInferredDependencies []models.UnresolvedJobDependency
	for _, inferredDependencyURN := range inferredDependencies {
		isResolved := false
		for _, resolvedDependency := range resolvedDependencies {
			if inferredDependencyURN == resolvedDependency.ResourceDestination {
				isResolved = true
				break
			}
		}
		if !isResolved {
			unresolvedInferredDependencies = append(unresolvedInferredDependencies, models.UnresolvedJobDependency{ResourceDestination: inferredDependencyURN})
		}
	}
	return unresolvedInferredDependencies
}

func (*dependencyResolver) groupDependencies(dependencyJobSpecs []models.JobSpec) map[string]models.JobSpecDependency {
	output := make(map[string]models.JobSpecDependency)
	for _, spec := range dependencyJobSpecs {
		projectSpec := spec.GetProjectSpec()
		jobSpec := spec

		key := projectSpec.Name + "/" + jobSpec.Name
		output[key] = models.JobSpecDependency{
			Project: &projectSpec,
			Job:     &jobSpec,
		}
	}
	return output
}

func (*dependencyResolver) fetchHookWithDependencies(jobSpec models.JobSpec) []models.JobSpecHook {
	var hooks []models.JobSpecHook
	for _, jobHook := range jobSpec.Hooks {
		jobHook.DependsOn = nil
		for _, depends := range jobHook.Unit.Info().DependsOn {
			dependentHook, err := jobSpec.GetHookByName(depends)
			if err == nil {
				jobHook.DependsOn = append(jobHook.DependsOn, &dependentHook)
			}
		}
		hooks = append(hooks, jobHook)
	}
	return hooks
}

func (*dependencyResolver) notifyProgress(observer progress.Observer, e progress.Event) {
	if observer == nil {
		return
	}
	observer.Notify(e)
}
