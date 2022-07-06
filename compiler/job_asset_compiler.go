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

func (c *JobRunAssetsCompiler) CompileNewJobRunAssets(ctx context.Context, jobSpec models.JobSpec, scheduledAt time.Time, jobRunSpec models.JobRunSpec, contextForTask map[string]interface{}) (map[string]string, error) {
	plugin, err := c.pluginRepo.GetByName(jobSpec.Task.Unit.Info().Name) // Do not access plugin from spec
	if err != nil {
		return nil, err
	}

	var inputFiles map[string]string
	instanceFileMap := getJobRunFiles(jobRunSpec)
	inputFiles = utils.MergeMaps(instanceFileMap, jobSpec.Assets.ToMap())

	if plugin.CLIMod != nil {
		// check if task needs to override the compilation behaviour
		compiledAssetResponse, err := plugin.CLIMod.CompileAssets(ctx, models.CompileAssetsRequest{
			Window:           jobSpec.Task.Window,
			Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			InstanceSchedule: scheduledAt,
			InstanceData:     jobRunSpec.Data,
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

func (c *JobRunAssetsCompiler) CompileJobRunAssets(ctx context.Context, jobRun models.JobRun, instanceSpec models.InstanceSpec, contextForTask map[string]interface{}) (map[string]string, error) {
	plugin, err := c.pluginRepo.GetByName(jobRun.Spec.Task.Unit.Info().Name) // Do not access plugin from spec
	if err != nil {
		return nil, err
	}

	var inputFiles map[string]string
	instanceFileMap := getInstanceFiles(instanceSpec)
	inputFiles = utils.MergeMaps(instanceFileMap, jobRun.Spec.Assets.ToMap())

	if plugin.CLIMod != nil {
		// check if task needs to override the compilation behaviour
		compiledAssetResponse, err := plugin.CLIMod.CompileAssets(ctx, models.CompileAssetsRequest{
			Window:           jobRun.Spec.Task.Window,
			Config:           models.PluginConfigs{}.FromJobSpec(jobRun.Spec.Task.Config),
			Assets:           models.PluginAssets{}.FromJobSpec(jobRun.Spec.Assets),
			InstanceSchedule: jobRun.ScheduledAt,
			InstanceData:     instanceSpec.Data,
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

func getInstanceFiles(instanceSpec models.InstanceSpec) map[string]string {
	if instanceSpec.Data == nil {
		return nil
	}
	fileMap := map[string]string{}
	for _, jobRunData := range instanceSpec.Data {
		if jobRunData.Type == models.InstanceDataTypeFile {
			fileMap[jobRunData.Name] = jobRunData.Value
		}
	}
	return fileMap
}

func getJobRunFiles(jobRunSpec models.JobRunSpec) map[string]string {
	if jobRunSpec.Data == nil {
		return nil
	}
	fileMap := map[string]string{}
	for _, jobRunData := range jobRunSpec.Data {
		if jobRunData.Type == models.InstanceDataTypeFile {
			fileMap[jobRunData.Name] = jobRunData.Value
		}
	}
	return fileMap
}
