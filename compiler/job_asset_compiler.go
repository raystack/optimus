package compiler

import (
	"context"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

type JobRunAssetsCompiler struct {
	engine     models.TemplateEngine
	pluginRepo models.PluginRepository
}

func NewJobAssetsCompiler(engine models.TemplateEngine, pluginRepo models.PluginRepository) *JobRunAssetsCompiler {
	return &JobRunAssetsCompiler{
		engine:     engine,
		pluginRepo: pluginRepo,
	}
}

func (c *JobRunAssetsCompiler) CompileJobRunAssets(ctx context.Context, jobSpec models.JobSpec, jobRunSpecData []models.JobRunSpecData, scheduledAt time.Time, contextForTask map[string]interface{}) (map[string]string, error) {
	plugin, err := c.pluginRepo.GetByName(jobSpec.Task.Unit.Info().Name) // Do not access plugin from spec
	if err != nil {
		return nil, err
	}

	var instanceFileMap map[string]string
	if jobRunSpecData == nil {
		instanceFileMap = nil
	} else {
		instanceFileMap = getFileMapFromJobRunSpecData(jobRunSpecData)
	}

	inputFiles := utils.MergeMaps(instanceFileMap, jobSpec.Assets.ToMap())

	if plugin.CLIMod != nil {
		// check if task needs to override the compilation behaviour
		compiledAssetResponse, err := plugin.CLIMod.CompileAssets(ctx, models.CompileAssetsRequest{
			Window:           jobSpec.Task.Window,
			Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			InstanceSchedule: scheduledAt,
			InstanceData:     jobRunSpecData,
		})
		if err != nil {
			return nil, err
		}
		inputFiles = utils.MergeMaps(instanceFileMap, compiledAssetResponse.Assets.ToJobSpec().ToMap())
	}

	fileMap, err := c.engine.CompileFiles(inputFiles, contextForTask)
	if err != nil {
		return nil, err
	}
	return fileMap, nil
}

func getFileMapFromJobRunSpecData(jobRunSpecData []models.JobRunSpecData) map[string]string {
	fileMap := map[string]string{}
	for _, jobRunData := range jobRunSpecData {
		if jobRunData.Type == models.InstanceDataTypeFile {
			fileMap[jobRunData.Name] = jobRunData.Value
		}
	}
	return fileMap
}

func getEnvMapFromJobRunSpecData(jobRunSpecData []models.JobRunSpecData) map[string]string {
	envMap := map[string]string{}
	for _, jobRunData := range jobRunSpecData {
		if jobRunData.Type == models.InstanceDataTypeEnv {
			envMap[jobRunData.Name] = jobRunData.Value
		}
	}
	return envMap
}
