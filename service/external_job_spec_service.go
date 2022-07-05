package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/rsrcmgr/neighbor"
)

// ExternalJobSpecService is service to handle job
type ExternalJobSpecService interface {
	GetExternalJobSpecsByDependentJobName(context.Context, models.ProjectID) (map[string][]models.JobSpec, error)
}

type resourceManagerConfig struct {
	Name    string
	Host    string
	Headers map[string]string
}

type externalJobSpecService struct {
	repositories map[string]store.ExternalJobSpecRepository
	configs      map[string]resourceManagerConfig

	unknownJobDependencyRepository store.UnknownJobDependencyRepository
}

func NewDefaultExternalJobSpecService(
	resourceManagers []config.ResourceManager,
	jobSourceRepository store.JobSourceRepository,
	unknownJobDependencyRepository store.UnknownJobDependencyRepository,
) (ExternalJobSpecService, error) {
	if jobSourceRepository == nil {
		return nil, errors.New("job source repository is nil")
	}
	if unknownJobDependencyRepository == nil {
		return nil, errors.New("unknown job dependency repository is nil")
	}
	repositories := make(map[string]store.ExternalJobSpecRepository)
	configs := make(map[string]resourceManagerConfig)
	for _, conf := range resourceManagers {
		switch conf.Type {
		case "optimus":
			var neighborConfig *config.ResourceManagerConfigOptimus
			if err := mapstructure.Decode(conf.Config, &neighborConfig); err != nil {
				return nil, fmt.Errorf("error encountered decoding config for optimus [%s]: %w", conf.Name, err)
			}
			repository, err := neighbor.NewJobSpecRepository(neighborConfig)
			if err != nil {
				return nil, fmt.Errorf("error constructing repository for optimus [%s]: %w", conf.Name, err)
			}
			repositories[conf.Name] = repository
			configs[conf.Name] = resourceManagerConfig{
				Name:    conf.Name,
				Host:    neighborConfig.Host,
				Headers: neighborConfig.Headers,
			}
		}
	}
	return &externalJobSpecService{
		repositories:                   repositories,
		configs:                        configs,
		unknownJobDependencyRepository: unknownJobDependencyRepository,
	}, nil
}

func (e *externalJobSpecService) GetExternalJobSpecsByDependentJobName(ctx context.Context, projectID models.ProjectID) (map[string][]models.JobSpec, error) {
	resourceDependencyNamesByJobName, err := e.unknownJobDependencyRepository.GetUnknownResourceDependencyNamesByJobName(ctx, projectID)
	if err != nil {
		return nil, err
	}
	resourceDependencyFiltersByJobName := e.toJobSpecFiltersByJobNameForResourceNames(resourceDependencyNamesByJobName)
	inferredNeighborDependenciesPerJobName, err := e.getInferredNeighborDependenciesPerJobName(ctx, resourceDependencyFiltersByJobName)
	if err != nil {
		return nil, err
	}

	staticDependencyNamesByJobID, err := e.unknownJobDependencyRepository.GetUnknownResourceDependencyNamesByJobName(ctx, projectID)
	if err != nil {
		return nil, err
	}
	staticDependencyFiltersByJobName, err := e.toJobSpecFiltersByJobNameForDependencyNames(staticDependencyNamesByJobID)
	if err != nil {
		return nil, err
	}
	// skip: job is skipped (including sensor)
	// ignore: sensor for job is not created, but job is deployed
	// if a value in static filters is empty, skip it
	return nil, nil
}

func (e *externalJobSpecService) getInferredNeighborDependenciesPerJobName(ctx context.Context, filtersPerJobName map[string][]models.JobSpecFilter) (map[string][]models.NeighborDependency, error) {
	output := make(map[string][]models.NeighborDependency)
	for jobName, filters := range filtersPerJobName {
		var neighborDependencies []models.NeighborDependency
		for _, filter := range filters {
			for managerName, repository := range e.repositories {
				specs, err := repository.GetJobSpecifications(ctx, filter)
				if err != nil {
					return nil, fmt.Errorf("error fetching job specifications from [%s] for job [%s]: %w", managerName, jobName, err)
				}
				for _, s := range specs {
					neighborDependencies = append(neighborDependencies, models.NeighborDependency{
						Name:          managerName,
						Host:          e.configs[managerName].Host,
						Headers:       e.configs[managerName].Headers,
						ProjectName:   s.GetProjectSpec().Name,
						NamespaceName: s.NamespaceSpec.Name,
						JobName:       s.Name,
					})
				}
			}
		}
		output[jobName] = neighborDependencies
	}
	return output, nil
}

func (e *externalJobSpecService) toJobSpecFiltersByJobNameForDependencyNames(dependencyNamesByJobName map[string][]string) (map[string][]models.JobSpecFilter, error) {
	output := make(map[string][]models.JobSpecFilter)
	var err error
	for jobName, dependencyNames := range dependencyNamesByJobName {
		var jobFilters []models.JobSpecFilter
		var invalidDependencyNames []string
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
		if len(invalidDependencyNames) > 0 {
			err = multierror.Append(err, fmt.Errorf("invalid static dependency names for [%s]: %s", jobName, strings.Join(invalidDependencyNames, ", ")))
		} else {
			output[jobName] = jobFilters
		}
	}
	return output, err
}

func (e *externalJobSpecService) toJobSpecFiltersByJobNameForResourceNames(resourceNamesByJobName map[string][]string) map[string][]models.JobSpecFilter {
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
