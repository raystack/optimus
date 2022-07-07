package job

import (
	"context"
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

	resolvers []OptimusDependencyResolver
}

// NewExternalDependencyResolver creates a new instance of externalDependencyResolver
func NewExternalDependencyResolver(resourceManagerConfigs []config.ResourceManager, unknownJobDependencyRepository store.UnknownJobDependencyRepository) (ExternalDependencyResolver, error) {
	resolvers := make([]OptimusDependencyResolver, len(resourceManagerConfigs))
	for i, conf := range resourceManagerConfigs {
		switch conf.Type {
		case "optimus":
			resolver, err := NewOptimusDependencyResolver(conf)
			if err != nil {
				return nil, err
			}
			resolvers[i] = resolver
		default:
			return nil, fmt.Errorf("resource manager [%s] is not recognized", conf.Type)
		}
	}
	return &externalDependencyResolver{
		unknownJobDependencyRepository: unknownJobDependencyRepository,
		resolvers:                      resolvers,
	}, nil
}

func (e *externalDependencyResolver) FetchInferredExternalDependenciesPerJobName(ctx context.Context, projectID models.ProjectID) (map[string]models.ExternalDependency, error) {
	unknownInferredDependenciesByJobName, err := e.unknownJobDependencyRepository.GetUnknownInferredDependencyURNsByJobName(ctx, projectID)
	if err != nil {
		return nil, err
	}
	jobSpecFiltersByJobName := e.toJobSpecFiltersByJobNameForResourceNames(unknownInferredDependenciesByJobName)
	return e.fetchInferredExternalDependencyPerJobName(ctx, jobSpecFiltersByJobName)
}

func (e *externalDependencyResolver) FetchStaticExternalDependenciesPerJobName(ctx context.Context, projectID models.ProjectID) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	unknownStaticDependenciesByJobName, err := e.unknownJobDependencyRepository.GetUnknownStaticDependencyNamesByJobName(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}
	jobSpecFiltersPerJobName, err := e.toJobSpecFiltersByJobNameForDependencyNames(unknownStaticDependenciesByJobName)
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
	for _, resolver := range e.resolvers {
		deps, err := resolver.FetchOptimusDependencies(ctx, filter)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, deps...)
	}
	return dependencies, nil
}

func (*externalDependencyResolver) toJobSpecFiltersByJobNameForResourceNames(resourceNamesByJobName map[string][]string) map[string][]models.JobSpecFilter {
	output := make(map[string][]models.JobSpecFilter)
	for jobName, resourceNames := range resourceNamesByJobName {
		for _, name := range resourceNames {
			filter := models.JobSpecFilter{
				ResourceDestination: name,
			}
			output[jobName] = append(output[jobName], filter)
		}
	}
	return output
}

func (e *externalDependencyResolver) toJobSpecFiltersByJobNameForDependencyNames(dependencyNamesByJobName map[string][]string) (map[string][]models.JobSpecFilter, error) {
	output := make(map[string][]models.JobSpecFilter)
	var err error
	for jobName, dependencyNames := range dependencyNamesByJobName {
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
