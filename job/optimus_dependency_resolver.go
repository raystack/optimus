package job

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/models"
	"golang.org/x/net/context"
)

type OptimusDependencyResolver interface {
	FetchExternalInferredDependenciesByJobName(context.Context, map[string][]models.JobSpecFilter) (map[string][]models.OptimusDependency, error)
	FetchExternalStaticDependenciesByJobName(context.Context, models.ProjectID) (map[string][]models.OptimusDependency, []models.UnknownDependency, error)
}

type optimusDependencyResolver struct {
	optimusResourceManager       resourcemgr.ResourceManager
	optimusResourceManagerConfig config.ResourceManagerConfigOptimus
}

// NewOptimusDependencyResolver creates a new instance of optimusDependencyResolver
func NewOptimusDependencyResolver(optimusResourceManager resourcemgr.ResourceManager) OptimusDependencyResolver {
	return &optimusDependencyResolver{
		optimusResourceManager: optimusResourceManager,
	}
}

func (o *optimusDependencyResolver) FetchExternalInferredDependenciesByJobName(ctx context.Context, jobSpecFilterPerJobName map[string][]models.JobSpecFilter) (map[string][]models.OptimusDependency, error) {
	optimusDependenciesPerJobName := make(map[string][]models.OptimusDependency)
	for jobName, filters := range jobSpecFilterPerJobName {
		var optimusDependencies []models.OptimusDependency
		for _, filter := range filters {
			jobSpecs, err := o.optimusResourceManager.GetJobSpecifications(ctx, filter)
			if err != nil {
				return nil, err
			}
			for _, spec := range jobSpecs {
				optimusDependencies = append(optimusDependencies, models.OptimusDependency{
					Name:          "",
					Host:          o.optimusResourceManagerConfig.Host,
					Headers:       o.optimusResourceManagerConfig.Headers,
					ProjectName:   spec.GetProjectSpec().Name,
					NamespaceName: spec.NamespaceSpec.Name,
					JobName:       spec.GetName(),
				})
			}
		}
		optimusDependenciesPerJobName[jobName] = optimusDependencies
	}
	return optimusDependenciesPerJobName, nil
}

func (o *optimusDependencyResolver) FetchExternalStaticDependenciesByJobName(ctx context.Context, projectID models.ProjectID) (map[string][]models.OptimusDependency, []models.UnknownDependency, error) {
	//TODO implement me
	panic("implement me")
}
