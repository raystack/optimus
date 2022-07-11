package job

import (
	"context"
	"errors"

	"github.com/mitchellh/mapstructure"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/resourcemgr"
	"github.com/odpf/optimus/models"
)

type OptimusDependencyGetter interface {
	GetOptimusDependencies(context.Context, models.JobSpecFilter) ([]models.OptimusDependency, error)
}

type optimusDependencyGetter struct {
	resourceManagerName   string
	resourceManagerConfig config.ResourceManagerConfigOptimus

	resourceManager resourcemgr.ResourceManager
}

// NewOptimusDependencyGetter creates a new instance of optimusDependencyGetter
func NewOptimusDependencyGetter(resourceManagerConfig config.ResourceManager) (OptimusDependencyGetter, error) {
	var resourceManagerOptimusConfig config.ResourceManagerConfigOptimus
	if err := mapstructure.Decode(resourceManagerConfig.Config, &resourceManagerOptimusConfig); err != nil {
		return nil, err
	}
	resourceManager, err := resourcemgr.NewOptimusResourceManager(resourceManagerOptimusConfig)
	if err != nil {
		return nil, err
	}
	return &optimusDependencyGetter{
		resourceManagerName:   resourceManagerConfig.Name,
		resourceManagerConfig: resourceManagerOptimusConfig,
		resourceManager:       resourceManager,
	}, nil
}

func (o *optimusDependencyGetter) GetOptimusDependencies(ctx context.Context, filter models.JobSpecFilter) ([]models.OptimusDependency, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	jobSpecs, err := o.resourceManager.GetJobSpecifications(ctx, filter)
	if err != nil {
		return nil, err
	}
	optimusDependencies := make([]models.OptimusDependency, len(jobSpecs))
	for i, spec := range jobSpecs {
		optimusDependencies[i] = models.OptimusDependency{
			Name:          o.resourceManagerName,
			Host:          o.resourceManagerConfig.Host,
			Headers:       o.resourceManagerConfig.Headers,
			ProjectName:   spec.GetProjectSpec().Name,
			NamespaceName: spec.NamespaceSpec.Name,
			JobName:       spec.GetName(),
		}
	}
	return optimusDependencies, nil
}
