package service

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/models"
)

type FilesCompiler interface {
	Compile(fileMap map[string]string, context map[string]any) (map[string]string, error)
}

type PluginRepo interface {
	GetByName(name string) (*models.Plugin, error)
}

type JobRunAssetsCompiler struct {
	compiler   FilesCompiler
	pluginRepo PluginRepo
}

func NewJobAssetsCompiler(engine FilesCompiler, pluginRepo PluginRepo) *JobRunAssetsCompiler {
	return &JobRunAssetsCompiler{
		compiler:   engine,
		pluginRepo: pluginRepo,
	}
}

func (c *JobRunAssetsCompiler) CompileJobRunAssets(ctx context.Context, job *scheduler.Job, systemEnvVars map[string]string, scheduledAt time.Time, contextForTask map[string]interface{}) (map[string]string, error) {
	plugin, err := c.pluginRepo.GetByName(job.Task.Name)
	if err != nil {
		return nil, err
	}

	startTime, err := job.Window.GetStartTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting start time: %w", err)
	}
	endTime, err := job.Window.GetEndTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting end time: %w", err)
	}

	inputFiles := job.Assets

	if plugin.DependencyMod != nil {
		// check if task needs to override the compilation behaviour
		compiledAssetResponse, err := plugin.DependencyMod.CompileAssets(ctx, models.CompileAssetsRequest{
			StartTime:    startTime,
			EndTime:      endTime,
			Config:       toPluginConfig(job.Task.Config),
			Assets:       toPluginAssets(job.Assets),
			InstanceData: toJobRunSpecData(systemEnvVars),
		})
		if err != nil {
			return nil, err
		}
		inputFiles = compiledAssetResponse.Assets.ToJobSpec().ToMap()
	}

	fileMap, err := c.compiler.Compile(inputFiles, contextForTask)
	if err != nil {
		return nil, err
	}
	return fileMap, nil
}

// TODO: deprecate after changing type for plugin
func toJobRunSpecData(mapping map[string]string) []models.JobRunSpecData {
	var jobRunData []models.JobRunSpecData
	for name, value := range mapping {
		jrData := models.JobRunSpecData{
			Name:  name,
			Value: value,
			Type:  models.InstanceDataTypeEnv,
		}
		jobRunData = append(jobRunData, jrData)
	}
	return jobRunData
}

// TODO: deprecate
func toPluginAssets(assets map[string]string) models.PluginAssets {
	var modelAssets models.PluginAssets
	for name, val := range assets {
		pa := models.PluginAsset{
			Name:  name,
			Value: val,
		}
		modelAssets = append(modelAssets, pa)
	}
	return modelAssets
}

// TODO: deprecate
func toPluginConfig(conf map[string]string) models.PluginConfigs {
	var pluginConfigs models.PluginConfigs
	for name, val := range conf {
		pc := models.PluginConfig{
			Name:  name,
			Value: val,
		}
		pluginConfigs = append(pluginConfigs, pc)
	}
	return pluginConfigs
}
