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
	FetchExternalInferredDependenciesPerJobName(context.Context, models.ProjectID) (map[string]models.ExternalDependency, error)
	FetchExternalStaticDependenciesPerJobName(context.Context, models.ProjectID) (map[string]models.ExternalDependency, []models.UnknownDependency, error)
}

type externalDependencyResolver struct {
	unknownJobDependencyRepository store.UnknownJobDependencyRepository

	resourceManagerConfigs []config.ResourceManager
}

// NewExternalDependencyResolver creates a new instance of externalDependencyResolver
func NewExternalDependencyResolver(unknownJobDependencyRepository store.UnknownJobDependencyRepository) (ExternalDependencyResolver, error) {

	// initializing multiple resolvers or multiple resource managers here

	return &externalDependencyResolver{
		unknownJobDependencyRepository: unknownJobDependencyRepository,
	}, nil
}

func (e *externalDependencyResolver) FetchExternalInferredDependenciesPerJobName(ctx context.Context, projectID models.ProjectID) (map[string]models.ExternalDependency, error) {
	//unknownDependenciesByJob, err := e.unknownJobDependencyRepository.GetUnknownInferredDependencyURNsByJobName(ctx, projectID)
	//if err != nil {
	//	return nil, err
	//}

	//jobSpecFiltersByJobName := e.toJobSpecFiltersByJobNameForResourceNames(unknownDependenciesByJob)

	// point to respective resolvers

	return nil, nil
}

func (e *externalDependencyResolver) FetchExternalStaticDependenciesPerJobName(ctx context.Context, projectID models.ProjectID) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	//staticDependencyNamesByJobID, err := e.unknownJobDependencyRepository.GetUnknownInferredDependencyURNsByJobName(ctx, projectID)
	//if err != nil {
	//	return nil, nil, err
	//}
	//jobSpecFiltersByJobName, err := e.toJobSpecFiltersByJobNameForDependencyNames(staticDependencyNamesByJobID)
	//if err != nil {
	//	return nil, nil, err
	//}

	// point to respective resolvers

	return nil, nil, nil
}

func (e *externalDependencyResolver) toJobSpecFiltersByJobNameForResourceNames(resourceNamesByJobName map[string][]string) map[string][]models.JobSpecFilter {
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
