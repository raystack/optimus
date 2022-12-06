package job

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"

	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/internal/writer"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
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

	externalDependencyResolver ExternalDependencyResolver
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver(
	jobSpecRepo store.JobSpecRepository,
	jobSourceRepo store.JobSourceRepository,
	pluginService service.PluginService,
	externalDependencyResolver ExternalDependencyResolver,
) DependencyResolver {
	return &dependencyResolver{
		jobSpecRepo:                jobSpecRepo,
		jobSourceRepo:              jobSourceRepo,
		pluginService:              pluginService,
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

	mergeInternalDependencies := func(input1, input2 map[string]models.JobSpecDependency) map[string]models.JobSpecDependency {
		for jobName, internalDependencies := range input2 {
			input1[jobName] = internalDependencies
		}
		return input1
	}

	// resolve inter/intra dependencies inferred by optimus
	jobSpec, err := d.resolveInferredDependencies(ctx, jobSpec, projectSpec, observer)
	if err != nil {
		return models.JobSpec{}, err
	}

	// resolve statically defined dependencies
	staticDependencies, _, err := d.GetStaticDependencies(ctx, jobSpec, projectSpec)
	if err != nil {
		return models.JobSpec{}, err
	}
	jobSpec.Dependencies = mergeInternalDependencies(jobSpec.Dependencies, staticDependencies)

	// resolve inter hook dependencies
	jobSpec = d.resolveHookDependencies(jobSpec)

	return jobSpec, nil
}

// GetStaticDependencies return named (explicit/static) dependencies that unresolved with its spec model
// this is normally happen when reading specs from a store[local/postgres]
// unresolved dependencies will no longer exist in the map
func (d *dependencyResolver) GetStaticDependencies(ctx context.Context, jobSpec models.JobSpec, projectSpec models.ProjectSpec) (map[string]models.JobSpecDependency, []models.OptimusDependency, error) {
	if ctx == nil {
		return nil, nil, errors.New("context is nil")
	}
	if reflect.ValueOf(jobSpec).IsZero() {
		return nil, nil, errors.New("job spec is empty")
	}
	if reflect.ValueOf(projectSpec).IsZero() {
		return nil, nil, errors.New("project spec is empty")
	}
	var err error
	resolvedJobSpecDependencies := make(map[string]models.JobSpecDependency)
	var externalOptimusDependencies []models.OptimusDependency
	for depName, depSpec := range jobSpec.Dependencies {
		resolvedJobSpecDependencies[depName] = depSpec
		if depSpec.Job == nil {
			switch depSpec.Type {
			case models.JobSpecDependencyTypeIntra:
				job, getJobError := d.jobSpecRepo.GetByNameAndProjectName(ctx, depName, projectSpec.Name)
				if getJobError != nil {
					err = multierror.Append(err, fmt.Errorf("%s for job %s: %w", ErrUnknownLocalDependency, depName, getJobError))
				} else {
					k := models.JobSpecDependency{
						Job:     &job,
						Project: &projectSpec,
						Type:    depSpec.Type,
					}
					resolvedJobSpecDependencies[depName] = k
				}
			case models.JobSpecDependencyTypeInter:
				// extract project name
				depParts := strings.SplitN(depName, "/", InterJobDependencyNameSections)
				if len(depParts) != InterJobDependencyNameSections {
					err = multierror.Append(err, fmt.Errorf("%s dependency should be in 'project_name/job_name' format: %s", models.JobSpecDependencyTypeInter, depName))
				} else {
					projectName := depParts[0]
					jobName := depParts[1]
					job, getJobError := d.jobSpecRepo.GetByNameAndProjectName(ctx, jobName, projectName)
					if getJobError != nil {
						unresolvedDependency, _ := convertDependencyNamesToUnresolvedJobDependency(depName)

						externalDependency, unresolved, errExternal := d.externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, map[string][]models.UnresolvedJobDependency{
							jobSpec.Name: {unresolvedDependency},
						})
						if errExternal != nil {
							err = multierror.Append(err, fmt.Errorf("%s for job %s: %w: %s", ErrUnknownCrossProjectDependency, depName, getJobError, errExternal.Error()))
						} else {
							dependencyResolvedFlag := true
							for _, dependency := range unresolved {
								if fmt.Sprintf("%s/%s", unresolvedDependency.ProjectName, unresolvedDependency.JobName) == fmt.Sprintf("%s/%s", dependency.DependencyProjectName, dependency.DependencyJobName) {
									// dependency could not be resolved
									err = multierror.Append(err, fmt.Errorf("%w for job %s", ErrUnknownCrossProjectDependency, unresolvedDependency.JobName))
									dependencyResolvedFlag = false
									continue
								}
							}
							if dependencyResolvedFlag {
								delete(resolvedJobSpecDependencies, depName)
								externalOptimusDependencies = append(externalOptimusDependencies, externalDependency[jobSpec.Name].OptimusDependencies...)
							}
						}
					} else {
						resolvedJobSpecDependencies[depName] = models.JobSpecDependency{
							Job:     &job,
							Project: &job.NamespaceSpec.ProjectSpec,
							Type:    depSpec.Type,
						}
					}
				}
			default:
				err = multierror.Append(err, fmt.Errorf("unsupported dependency type: %s", depSpec.Type))
			}
		}
	}

	return resolvedJobSpecDependencies, externalOptimusDependencies, err
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
		dependencyJobSpecs, err := d.jobSpecRepo.GetByResourceDestinationURN(ctx, depDestination)
		if err != nil {
			if errors.Is(err, store.ErrResourceNotFound) {
				// should not fail for unknown dependency, its okay to not have a upstream job
				// registered in optimus project and still refer to them in our job
				d.notifyProgress(observer, &models.ProgressJobSpecUnknownDependencyUsed{Job: jobSpec.Name, Dependency: depDestination})
				continue
			}
			return jobSpec, fmt.Errorf("runtime dependency evaluation failed: %w", err)
		}
		for _, dependencyJobSpec := range dependencyJobSpecs {
			dep := extractDependency(dependencyJobSpec, projectSpec)
			jobSpec.Dependencies[dep.Job.Name] = dep
		}
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

func (d *dependencyResolver) GetJobSpecsWithDependencies(ctx context.Context, projectName string) ([]models.JobSpec, []models.UnknownDependency, error) {
	if ctx == nil {
		return nil, nil, errors.New("context is nil")
	}
	if projectName == "" {
		return nil, nil, errors.New("project name is empty")
	}
	jobSpecs, err := d.jobSpecRepo.GetAllByProjectName(ctx, projectName)
	if err != nil {
		return nil, nil, err
	}

	internalDependenciesByJobID, err := d.getInternalDependenciesByJobID(ctx, projectName)
	if err != nil {
		return nil, nil, err
	}
	externalDependenciesByJobName, unknownDependencies, err := d.getExternalDependenciesByJobName(ctx, projectName, jobSpecs, internalDependenciesByJobID)
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

func (d *dependencyResolver) getInternalDependenciesByJobID(ctx context.Context, projectName string) (map[uuid.UUID][]models.JobSpec, error) {
	mergeInternalDependencies := func(input1, input2 map[uuid.UUID][]models.JobSpec) map[uuid.UUID][]models.JobSpec {
		for jobID, internalDependencies := range input2 {
			input1[jobID] = append(input1[jobID], internalDependencies...)
		}
		return input1
	}

	staticDependenciesPerJobID, err := d.jobSpecRepo.GetStaticDependenciesPerJobID(ctx, projectName)
	if err != nil {
		return nil, err
	}
	inferredDependenciesPerJobID, err := d.jobSpecRepo.GetInferredDependenciesPerJobID(ctx, projectName)
	if err != nil {
		return nil, err
	}

	return mergeInternalDependencies(staticDependenciesPerJobID, inferredDependenciesPerJobID), nil
}

// GetJobsByResourceDestinations get job spec of jobs that write to these destinations
func (d *dependencyResolver) getJobsByResourceDestinations(ctx context.Context, upstreamDestinations []string,
	subjectJobName string, logWriter writer.LogWriter) ([]models.JobSpec, error) {
	jobSpecDependencyList := []models.JobSpec{}
	for _, depDestination := range upstreamDestinations {
		dependencyJobSpec, err := d.jobSpecRepo.GetByResourceDestinationURN(ctx, depDestination)
		if err != nil {
			if errors.Is(err, store.ErrResourceNotFound) {
				// should not fail for unknown dependency, its okay to not have a upstream job
				// registered in optimus project and still refer to them in our job
				event := &models.ProgressJobSpecUnknownDependencyUsed{
					Job:        subjectJobName,
					Dependency: depDestination,
				}
				logWriter.Write(writer.LogLevelError, event.String())
				continue
			}
			return jobSpecDependencyList, fmt.Errorf("runtime dependency evaluation failed: %w", err)
		}
		jobSpecDependencyList = append(jobSpecDependencyList, dependencyJobSpec...)
	}
	return jobSpecDependencyList, nil
}

func (d *dependencyResolver) GetEnrichedUpstreamJobSpec(ctx context.Context, subjectJobSpec models.JobSpec,
	upstreamDestinations []string, logWriter writer.LogWriter) (models.JobSpec, []models.UnknownDependency, error) {
	var unknownDependencies []models.UnknownDependency
	inferredInternalUpstreams, err := d.getJobsByResourceDestinations(ctx,
		upstreamDestinations,
		subjectJobSpec.Name,
		logWriter)
	if err != nil {
		return subjectJobSpec, unknownDependencies, err
	}
	groupedInferredInternalUpstreams := d.groupDependencies(inferredInternalUpstreams)

	for jobName, dependecySpec := range groupedInferredInternalUpstreams {
		subjectJobSpec.Dependencies[jobName] = dependecySpec
	}

	var resolvedDependencies []models.JobSpec
	for _, dependency := range subjectJobSpec.Dependencies {
		if dependency.Job != nil {
			resolvedDependencies = append(resolvedDependencies, *dependency.Job)
		}
	}
	resolvedDependenciesByJobID := map[uuid.UUID][]models.JobSpec{
		subjectJobSpec.ID: resolvedDependencies,
	}

	externalDependenciesByJobName, unknownDependencies, err := d.getExternalDependenciesByJobName(ctx,
		subjectJobSpec.GetProjectSpec().Name,
		[]models.JobSpec{subjectJobSpec},
		resolvedDependenciesByJobID)
	if err != nil {
		return subjectJobSpec, unknownDependencies, err
	}

	subjectJobSpec.ExternalDependencies.OptimusDependencies = append(
		subjectJobSpec.ExternalDependencies.OptimusDependencies,
		externalDependenciesByJobName[subjectJobSpec.Name].OptimusDependencies...)

	subjectJobSpec.Hooks = d.fetchHookWithDependencies(subjectJobSpec)

	return subjectJobSpec, unknownDependencies, err
}

func (d *dependencyResolver) getExternalDependenciesByJobName(ctx context.Context, projectName string, jobSpecs []models.JobSpec, internalJobDependencies map[uuid.UUID][]models.JobSpec) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
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

	unresolvedStaticDependenciesPerJobName, unknownStaticDependenciesFromInternal := d.getUnresolvedStaticDependencies(jobSpecs, internalJobDependencies)
	staticExternalDependencyPerJobName, unknownStaticDependenciesFromExternal, err := d.externalDependencyResolver.FetchStaticExternalDependenciesPerJobName(ctx, unresolvedStaticDependenciesPerJobName)
	if err != nil {
		return nil, nil, err
	}
	unknownStaticDependencies := mergeUnknownDependencies(unknownStaticDependenciesFromInternal, unknownStaticDependenciesFromExternal)

	unresolvedInferredDependenciesPerJobName, err := d.getUnresolvedInferredDependencies(ctx, projectName, jobSpecs, internalJobDependencies)
	if err != nil {
		return nil, nil, err
	}
	inferredExternalDependencyPerJobName, err := d.externalDependencyResolver.FetchInferredExternalDependenciesPerJobName(ctx, unresolvedInferredDependenciesPerJobName)
	if err != nil {
		return nil, nil, err
	}

	externalDependenciesByJobName := mergeExternalDependencies(staticExternalDependencyPerJobName, inferredExternalDependencyPerJobName)
	return externalDependenciesByJobName, unknownStaticDependencies, nil
}

func (d *dependencyResolver) getUnresolvedStaticDependencies(jobSpecs []models.JobSpec, internalJobDependencies map[uuid.UUID][]models.JobSpec) (map[string][]models.UnresolvedJobDependency, []models.UnknownDependency) {
	unresolvedStaticDependenciesPerJobName := make(map[string][]models.UnresolvedJobDependency)
	var unknownDependencies []models.UnknownDependency
	for _, jobSpec := range jobSpecs {
		resolvedDependencies := internalJobDependencies[jobSpec.ID]
		unresolvedStaticDependencies, unknownStaticDependencyNames := d.identifyUnresolvedStaticDependencies(jobSpec.Dependencies, resolvedDependencies)

		unknownDependenciesPerJob := d.convertToUnknownDependencies(unknownStaticDependencyNames, jobSpec.Name, jobSpec.GetProjectSpec().Name)
		unknownDependencies = append(unknownDependencies, unknownDependenciesPerJob...)

		if len(unresolvedStaticDependencies) > 0 {
			unresolvedStaticDependenciesPerJobName[jobSpec.Name] = unresolvedStaticDependencies
		}
	}
	return unresolvedStaticDependenciesPerJobName, unknownDependencies
}

func (*dependencyResolver) convertToUnknownDependencies(unknownStaticDependencyNames []string, jobName, projectName string) []models.UnknownDependency {
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

func (d *dependencyResolver) getUnresolvedInferredDependencies(ctx context.Context, projectName string,
	jobSpecs []models.JobSpec, internalJobDependencies map[uuid.UUID][]models.JobSpec) (map[string][]models.UnresolvedJobDependency, error) {
	inferredDependencyURNsPerJobID, err := d.jobSourceRepo.GetResourceURNsPerJobID(ctx, projectName)
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

func (d *dependencyResolver) GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time) ([]models.JobRun, error) {
	return d.externalDependencyResolver.GetExternalJobRuns(ctx, host, jobName, projectName, startDate, endDate)
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
