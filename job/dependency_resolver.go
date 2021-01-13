package job

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

var (
	ErrUnknownDependency = errors.New("unknown dependency")
)

type dependencyResolver struct {
}

// Resolve resolves dependency inbetween specs passed as args
// if a jobSpec refer to a dependency that is not passed as args, it will be
// ignored so ideally this is built to pass all specs at a time and resolve
// dependencies of all specs of a single project
func (r *dependencyResolver) Resolve(jobSpecs []models.JobSpec) ([]models.JobSpec, error) {
	// build map of jobDestination => models.JobSpec
	jobToDestinationMap, err := r.getJobToDestinationMap(jobSpecs)
	if err != nil {
		return nil, err
	}

	// build map of jobName => models.JobSpec
	jobSpecMapByName := map[string]models.JobSpec{}
	for _, jobSpec := range jobSpecs {
		jobSpecMapByName[jobSpec.Name] = jobSpec
	}

	// resolve dependencies inferred from all optimus jobs
	jobSpecs, err = r.resolveInferredDependencies(jobSpecs, jobToDestinationMap)
	if err != nil {
		return nil, err
	}

	// resolve statically defined dependencies
	jobSpecs, err = r.resolveStaticDependencies(jobSpecs, jobSpecMapByName)
	if err != nil {
		return nil, err
	}

	return jobSpecs, nil
}

func (r *dependencyResolver) getJobToDestinationMap(jobSpecs []models.JobSpec) (map[string]models.JobSpec, error) {
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
	return jobToDestinationMap, nil
}

func (r *dependencyResolver) resolveInferredDependencies(jobSpecs []models.JobSpec, jobToDestinationMap map[string]models.JobSpec) ([]models.JobSpec, error) {
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

		jobSpecs[jobIdx] = jobSpec
	}
	return jobSpecs, nil
}

func (r *dependencyResolver) resolveStaticDependencies(jobSpecs []models.JobSpec, jobSpecMapByName map[string]models.JobSpec) ([]models.JobSpec, error) {
	// update static dependencies if unresolved with its spec model
	for jobIdx, jobSpec := range jobSpecs {
		for depName, depSpec := range jobSpec.Dependencies {
			if depSpec.Job == nil {
				job, ok := jobSpecMapByName[depName]
				if !ok {
					return jobSpecs, errors.Wrap(ErrUnknownDependency, depName)
				}
				depSpec.Job = &job
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
