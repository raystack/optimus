package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/models"
)

type ExternalDependencyResolver interface {
	FetchInferredExternalDependenciesPerJobName(ctx context.Context, unresolvedDependenciesPerJobName map[string][]models.UnresolvedJobDependency) (map[string]models.ExternalDependency, error)
	FetchStaticExternalDependenciesPerJobName(ctx context.Context, unresolvedDependenciesPerJobName map[string][]models.UnresolvedJobDependency) (map[string]models.ExternalDependency, []models.UnknownDependency, error)
	GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time) ([]models.JobRun, error)
}

type externalDependencyResolver struct {
	optimusResourceManagers []resourcemgr.ResourceManager
}

// NewExternalDependencyResolver creates a new instance of externalDependencyResolver
func NewExternalDependencyResolver(resourceManagerConfigs []config.ResourceManager) (ExternalDependencyResolver, error) {
	var optimusResourceManagers []resourcemgr.ResourceManager
	for _, conf := range resourceManagerConfigs {
		switch conf.Type {
		case "optimus":
			getter, err := resourcemgr.NewOptimusResourceManager(conf)
			if err != nil {
				return nil, err
			}
			optimusResourceManagers = append(optimusResourceManagers, getter)
		default:
			return nil, fmt.Errorf("resource manager [%s] is not recognized", conf.Type)
		}
	}
	return &externalDependencyResolver{
		optimusResourceManagers: optimusResourceManagers,
	}, nil
}

func (e *externalDependencyResolver) FetchInferredExternalDependenciesPerJobName(ctx context.Context, unresolvedDependenciesPerJobName map[string][]models.UnresolvedJobDependency) (map[string]models.ExternalDependency, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	externalDependencyPerJobName := make(map[string]models.ExternalDependency)
	for jobName, filters := range unresolvedDependenciesPerJobName {
		optimusDependencies, err := e.fetchInferredOptimusDependencies(ctx, filters)
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

func (e *externalDependencyResolver) FetchStaticExternalDependenciesPerJobName(ctx context.Context, unresolvedDependenciesPerJobName map[string][]models.UnresolvedJobDependency) (map[string]models.ExternalDependency, []models.UnknownDependency, error) {
	if ctx == nil {
		return nil, nil, errors.New("context is nil")
	}

	var unknownDependencies []models.UnknownDependency
	externalDependencyPerJobName := make(map[string]models.ExternalDependency)
	for jobName, toBeResolvedDependency := range unresolvedDependenciesPerJobName {
		optimusDependencies, unresolvedFromExternal, err := e.fetchStaticOptimusDependencies(ctx, toBeResolvedDependency)
		if err != nil {
			return nil, nil, err
		}
		unknownDependenciesFromUnresolved := e.convertUnresolvedToUnknownDependencies(jobName, unresolvedFromExternal)
		unknownDependencies = append(unknownDependencies, unknownDependenciesFromUnresolved...)
		// external dependency types other than optimus will be called in this line, and used in the line below
		externalDependencyPerJobName[jobName] = models.ExternalDependency{
			OptimusDependencies: optimusDependencies,
		}
	}

	return externalDependencyPerJobName, unknownDependencies, nil
}

func (*externalDependencyResolver) convertUnresolvedToUnknownDependencies(jobName string, unresolvedDependencies []models.UnresolvedJobDependency) []models.UnknownDependency {
	unknownDependencies := make([]models.UnknownDependency, len(unresolvedDependencies))
	for i := 0; i < len(unresolvedDependencies); i++ {
		unknownDependencies[i] = models.UnknownDependency{
			JobName:               jobName,
			DependencyProjectName: unresolvedDependencies[i].ProjectName,
			DependencyJobName:     unresolvedDependencies[i].JobName,
		}
	}
	return unknownDependencies
}

func (e *externalDependencyResolver) fetchInferredOptimusDependencies(ctx context.Context, unresolvedDependencies []models.UnresolvedJobDependency) ([]models.OptimusDependency, error) {
	var optimusDependencies []models.OptimusDependency
	for _, unresolvedDependency := range unresolvedDependencies {
		dependencies, err := e.fetchOptimusDependencies(ctx, unresolvedDependency)
		if err != nil {
			return nil, err
		}
		optimusDependencies = append(optimusDependencies, dependencies...)
	}
	return optimusDependencies, nil
}

func (e *externalDependencyResolver) GetExternalJobRuns(ctx context.Context, host, jobName, projectName string, startDate, endDate time.Time) ([]models.JobRun, error) {
	for _, manager := range e.optimusResourceManagers {
		if manager.GetHost() == host {
			return manager.GetExternalJobRuns(ctx, host, jobName, projectName, startDate, endDate)
		}
	}
	return []models.JobRun{}, fmt.Errorf("could not find optimus external resoruce manager with host:%s", host)
}

func (e *externalDependencyResolver) fetchStaticOptimusDependencies(ctx context.Context, unresolvedDependencies []models.UnresolvedJobDependency) ([]models.OptimusDependency, []models.UnresolvedJobDependency, error) {
	var optimusDependencies []models.OptimusDependency
	var unresolvedFromExternal []models.UnresolvedJobDependency
	for _, toBeResolvedDependency := range unresolvedDependencies {
		dependencies, err := e.fetchOptimusDependencies(ctx, toBeResolvedDependency)
		if err != nil {
			return nil, nil, err
		}
		if len(dependencies) == 0 {
			unresolvedFromExternal = append(unresolvedFromExternal, toBeResolvedDependency)
			continue
		}
		optimusDependencies = append(optimusDependencies, dependencies...)
	}
	return optimusDependencies, unresolvedFromExternal, nil
}

func (e *externalDependencyResolver) fetchOptimusDependencies(ctx context.Context, unresolvedDependency models.UnresolvedJobDependency) ([]models.OptimusDependency, error) {
	var dependencies []models.OptimusDependency
	for _, manager := range e.optimusResourceManagers {
		deps, err := manager.GetOptimusDependencies(ctx, unresolvedDependency)
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, deps...)
	}
	return dependencies, nil
}
