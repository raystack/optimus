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

	startTime, err := jobSpec.Task.Window.GetStartTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting start time: %w", err)
	}
	endTime, err := jobSpec.Task.Window.GetEndTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting end time: %w", err)
	}

	assetsToDump := jobSpec.Assets.ToMap()

	if allowOverride {
		// check if task needs to override the compilation behaviour
		compiledAssetResponse, err := jobSpec.Task.Unit.CLIMod.CompileAssets(ctx, models.CompileAssetsRequest{
			StartTime: startTime,
			EndTime:   endTime,
			Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
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
		models.ConfigKeyDstart:        startTime.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyDend:          endTime.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyExecutionTime: scheduledAt.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyDestination:   jobDestination,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to compile templates: %w", err)
	}

	return templates, nil
}
