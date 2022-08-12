package compiler

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/models"
)

// DumpAssets used for dry run and does not effect actual execution of a job
func DumpAssets(ctx context.Context, jobSpec models.JobSpec, scheduledAt time.Time, engine models.TemplateEngine, allowOverride bool) (map[string]string, error) {
	var jobDestination string
	if jobSpec.Task.Unit.DependencyMod != nil {
		jobDestinationResponse, err := jobSpec.Task.Unit.DependencyMod.GenerateDestination(ctx, models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			PluginOptions: models.PluginOptions{
				DryRun: true,
			},
		})
		if err != nil {
			return nil, err
		}
		jobDestination = jobDestinationResponse.Destination
	}

	assetsToDump := jobSpec.Assets.ToMap()

	if allowOverride {
		// check if task needs to override the compilation behaviour
		compiledAssetResponse, err := jobSpec.Task.Unit.CLIMod.CompileAssets(ctx, models.CompileAssetsRequest{
			Window:           jobSpec.Task.Window,
			Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			InstanceSchedule: scheduledAt,
			InstanceData: []models.JobRunSpecData{
				{
					Name:  models.ConfigKeyExecutionTime,
					Value: scheduledAt.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
			},
			PluginOptions: models.PluginOptions{
				DryRun: true,
			},
		})
		if err != nil {
			return nil, err
		}
		assetsToDump = compiledAssetResponse.Assets.ToJobSpec().ToMap()
	}

	// compile again if needed
	templates, err := engine.CompileFiles(assetsToDump, map[string]interface{}{
		models.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyExecutionTime: scheduledAt.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyDestination:   jobDestination,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to compile templates: %w", err)
	}

	return templates, nil
}
