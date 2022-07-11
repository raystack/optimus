package job

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type ExternalDependencyResolver interface {
	FetchInferredExternalDependenciesPerJobName(context.Context, models.ProjectID) (map[string]models.ExternalDependency, error)
	FetchStaticExternalDependenciesPerJobName(context.Context, models.ProjectID) (map[string]models.ExternalDependency, []models.UnknownDependency, error)
}

type externalDependencyResolver struct {
	unknownJobDependencyRepository store.UnknownJobDependencyRepository

	optimusDependencyGetters []OptimusDependencyGetter
}

// NewExternalDependencyResolver creates a new instance of externalDependencyResolver
func NewExternalDependencyResolver(resourceManagerConfigs []config.ResourceManager, unknownJobDependencyRepository store.UnknownJobDependencyRepository) (ExternalDependencyResolver, error) {
	var optimusDependencyGetters []OptimusDependencyGetter
	for _, conf := range resourceManagerConfigs {
		switch conf.Type {
		case "optimus":
			getter, err := NewOptimusDependencyGetter(conf)
			if err != nil {
				return nil, err
			}
			optimusDependencyGetters = append(optimusDependencyGetters, getter)
		default:
			return nil, fmt.Errorf("resource manager [%s] is not recognized", conf.Type)
		}
	}
	return &externalDependencyResolver{
		unknownJobDependencyRepository: unknownJobDependencyRepository,
		optimusDependencyGetters:       optimusDependencyGetters,
	}, nil
}

func (e *externalDependencyResolver) FetchInferredExternalDependenciesPerJobName(ctx context.Context, projectID models.ProjectID) (map[string]models.ExternalDependency, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	unknownInferredDependenciesPerJobName, err := e.unknownJobDependencyRepository.GetUnknownInferredDependencyURNsPerJobName(ctx, projectID)
	if err != nil {
		return nil, err
	}
	jobSpecFiltersPerJobName := e.toJobSpecFiltersPerJobNameForResourceNames(unknownInferredDependenciesPerJobName)
	return e.fetchInferredExternalDependencyPerJobName(ctx, jobSpecFiltersPerJobName)
}

func (e *externalDependencyResolver) FetchStaticExternalDependenciesPerJobName(ctx context.Context, projectID models.ProjectID) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	if ctx == nil {
		return nil, nil, errors.New("context is nil")
	}
	unknownStaticDependenciesPerJobName, err := e.unknownJobDependencyRepository.GetUnknownStaticDependencyNamesPerJobName(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}
	jobSpecFiltersPerJobName, err := e.toJobSpecFiltersPerJobNameForDependencyNames(unknownStaticDependenciesPerJobName)
	if err != nil {
		return nil, nil, err
	}
	return e.fetchStaticExternalDependencyPerJobName(ctx, jobSpecFiltersPerJobName)
}

func (e *externalDependencyResolver) fetchInferredExternalDependencyPerJobName(ctx context.Context, jobSpecFiltersPerJobName map[string][]models.JobSpecFilter) (map[string]models.ExternalDependency, error) {
	externalDependencyPerJobName := make(map[string]models.ExternalDependency)
	for jobName, filters := range jobSpecFiltersPerJobName {
		optimusDependencies, err := e.fetchInferredOptimusDependenciesForFilters(ctx, filters)
		if err != nil {
			return nil, err
		}
		// external dependency types other than optimus will be called in this line, and used in the line below
		externalDependencyPerJobName[jobName] = models.ExternalDependency{
			OptimusDependencies: optimusDependencies,
		}
	}
	return externalDependencyPerJobName, nil
}

func (e *externalDependencyResolver) fetchInferredOptimusDependenciesForFilters(ctx context.Context, filters []models.JobSpecFilter) ([]models.OptimusDependency, error) {
	var optimusDependencies []models.OptimusDependency
	for _, filter := range filters {
		dependencies, err := e.fetchOptimusDependenciesPerFilter(ctx, filter)
		if err != nil {
			return nil, err
		}
		optimusDependencies = append(optimusDependencies, dependencies...)
	}
	return optimusDependencies, nil
}

func (e *externalDependencyResolver) fetchStaticExternalDependencyPerJobName(ctx context.Context, jobSpecFiltersPerJobName map[string][]models.JobSpecFilter) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	var allUnknownDependencies []models.UnknownDependency
	externalDependencyPerJobName := make(map[string]models.ExternalDependency)
	for jobName, filters := range jobSpecFiltersPerJobName {
		optimusDependencies, unknownDependencies, err := e.fetchStaticOptimusDependenciesForFilters(ctx, jobName, filters)
		if err != nil {
			return nil, nil, err
		}
		// external dependency types other than optimus will be called in this line, and used in the line below
		externalDependencyPerJobName[jobName] = models.ExternalDependency{
			OptimusDependencies: optimusDependencies,
		}
		allUnknownDependencies = append(allUnknownDependencies, unknownDependencies...)
	}
	return externalDependencyPerJobName, allUnknownDependencies, nil
}

func (e *externalDependencyResolver) fetchStaticOptimusDependenciesForFilters(ctx context.Context, jobName string, filters []models.JobSpecFilter) ([]models.OptimusDependency, []models.UnknownDependency, error) {
	var optimusDependencies []models.OptimusDependency
	var unknownDependencies []models.UnknownDependency
	for _, filter := range filters {
		dependencies, err := e.fetchOptimusDependenciesPerFilter(ctx, filter)
		if err != nil {
			return nil, nil, err
		}
		if len(dependencies) == 0 {
			unknownDependencies = append(unknownDependencies, models.UnknownDependency{
				JobName:               jobName,
				DependencyProjectName: filter.ProjectName,
				DependencyJobName:     filter.JobName,
			})
			continue
		}
		optimusDependencies = append(optimusDependencies, dependencies...)
	}
	return optimusDependencies, unknownDependencies, nil
}

func (e *externalDependencyResolver) fetchOptimusDependenciesPerFilter(ctx context.Context, filter models.JobSpecFilter) ([]models.OptimusDependency, error) {
	var dependencies []models.OptimusDependency
	for _, getter := range e.optimusDependencyGetters {
		deps, err := getter.GetOptimusDependencies(ctx, filter)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, deps...)
	}
	return dependencies, nil
}

func (*externalDependencyResolver) toJobSpecFiltersPerJobNameForResourceNames(resourceNamesPerJobName map[string][]string) map[string][]models.JobSpecFilter {
	output := make(map[string][]models.JobSpecFilter)
	for jobName, resourceNames := range resourceNamesPerJobName {
		for _, name := range resourceNames {
			filter := models.JobSpecFilter{
				ResourceDestination: name,
			}
			output[jobName] = append(output[jobName], filter)
		}
	}
	return output
}

func (e *externalDependencyResolver) toJobSpecFiltersPerJobNameForDependencyNames(dependencyNamesPerJobName map[string][]string) (map[string][]models.JobSpecFilter, error) {
	output := make(map[string][]models.JobSpecFilter)
	var err error
	for jobName, dependencyNames := range dependencyNamesPerJobName {
		jobFilters, invalidDependencyNames := e.convertDependencyNamesToFilters(dependencyNames)
		if len(invalidDependencyNames) > 0 {
			// TODO: should consider what treatment to be done, currently we are "error"-ing it
			err = multierror.Append(err, fmt.Errorf("invalid static dependency names for [%s]: %s", jobName, strings.Join(invalidDependencyNames, ", ")))
		} else {
			output[jobName] = jobFilters
		}
	}
	return output, err
}

func (*externalDependencyResolver) convertDependencyNamesToFilters(dependencyNames []string) (jobFilters []models.JobSpecFilter, invalidDependencyNames []string) {
	for _, name := range dependencyNames {
		splitName := strings.Split(name, "/")
		expectedSplintLen := 2
		if len(splitName) != expectedSplintLen {
			invalidDependencyNames = append(invalidDependencyNames, name)
			continue
		}
		filter := models.JobSpecFilter{
			ProjectName: splitName[0],
			JobName:     splitName[1],
		}
		jobFilters = append(jobFilters, filter)
	}
	return
}
