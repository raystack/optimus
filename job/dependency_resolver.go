package job

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

type dependencyResolver struct {
	supportedTaskRepo models.SupportedTaskRepo
}

func (r *dependencyResolver) Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	// prepare jobs destination
	jobToDestinationMap := map[string]models.JobSpec{}
	for _, jobSpec := range jobSpecs {
		jobDestination, err := jobSpec.Task.Unit.GenerateDestination(models.UnitData{
			Config: jobSpec.Task.Config,
			Assets: jobSpec.Assets.ToMap(),
		})
		if err != nil {
			return nil, err
		}
		jobToDestinationMap[jobDestination] = jobSpec
	}

	// get dependencies
	for jobIdx, jobSpec := range jobSpecs {
		// get destinations of dependencies
		jobDependenciesDestination, err := jobSpec.Task.Unit.GenerateDependencies(models.UnitData{
			Config: jobSpec.Task.Config,
			Assets: jobSpec.Assets.ToMap(),
		})
		if err != nil {
			return nil, err
		}

		// get job spec of these destinations and append to current jobSpec
		// this will resolve runtime dependencies
		for _, depDestination := range jobDependenciesDestination {
			depSpec, ok := jobToDestinationMap[depDestination]
			if !ok {
				return jobSpecs, errors.Errorf("invalid job specs, undefined destination %s", depDestination)
			}
			jobSpec.Dependencies[depSpec.Name] = models.JobSpecDependency{
				Job: &depSpec,
			}
		}

		// update static dependencies if unresolved with its spec model
		for depName, depSpec := range jobSpec.Dependencies {
			if depSpec.Job == nil {
				for _, job := range jobSpecs {
					if job.Name == depName {
						depSpec.Job = &job
						break
					}
				}
				jobSpec.Dependencies[depName] = depSpec
			}
		}

		jobSpecs[jobIdx] = jobSpec
	}
	return jobSpecs, nil
}

// NewDependencyResolver creates a new instance of Resolver
func NewDependencyResolver() *dependencyResolver {
	return &dependencyResolver{}
}
