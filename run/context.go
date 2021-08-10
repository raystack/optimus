package run

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	// TaskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	TaskConfigPrefix = "TASK__"

	// ProjectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	ProjectConfigPrefix = "GLOBAL__"
)

var (
	// IgnoreTemplateRenderExtension used as extension on a file will skip template
	// rendering of it
	IgnoreTemplateRenderExtension = []string{".gtpl", ".j2", ".tmpl", ".tpl"}
)

// ContextManager fetches all config data for a given instanceSpec and compiles all
// macros/templates.
// Context here is a term used for the input required for tasks to execute.
// Raw task assets may not be executable in there default state and needs to be
// transformed before they can work as inputs. Input could be through
// environment variables or as a file.
// It exposes .proj, .inst, .task template variable names containing configs that can be
// used in job specification
type ContextManager struct {
	namespace models.NamespaceSpec
	jobRun    models.JobRun
	engine    models.TemplateEngine
}

// Generate fetches and compiles all config data related to an instance and
// returns a map of env variables and a map[fileName]fileContent
// It compiles any templates/macros present in the config.
func (fm *ContextManager) Generate(instanceSpec models.InstanceSpec) (
	envMap map[string]string, fileMap map[string]string, err error) {
	projectPrefixedConfig, projRawConfig := fm.projectEnvs()

	// instance env will be used for templating
	instanceEnvMap, instanceFileMap := fm.getInstanceData(instanceSpec)

	// merge both
	projectInstanceContext := MergeInterfaceMapToInterface(instanceEnvMap, projectPrefixedConfig)
	projectInstanceContext["proj"] = projRawConfig
	projectInstanceContext["inst"] = instanceEnvMap

	// prepare configs
	envMap, err = fm.generateEnvs(instanceSpec.Name, instanceSpec.Type, projectInstanceContext)
	if err != nil {
		return nil, nil, err
	}

	// append instance envMap
	for k, v := range instanceEnvMap {
		if vs, ok := v.(string); ok {
			envMap[k] = vs
		}
	}

	// do the same for asset files
	// check if task needs to override the compilation behaviour
	compiledAssetResponse, err := fm.jobRun.Spec.Task.Unit.CLIMod.CompileAssets(context.Background(), models.CompileAssetsRequest{
		Window:           fm.jobRun.Spec.Task.Window,
		Config:           models.PluginConfigs{}.FromJobSpec(fm.jobRun.Spec.Task.Config),
		Assets:           models.PluginAssets{}.FromJobSpec(fm.jobRun.Spec.Assets),
		InstanceSchedule: fm.jobRun.ScheduledAt,
		InstanceData:     instanceSpec.Data,
	})
	if err != nil {
		return nil, nil, err
	}

	// append job spec assets to list of files need to write
	fileMap = MergeStringMap(instanceFileMap, compiledAssetResponse.Assets.ToJobSpec().ToMap())
	if fileMap, err = fm.engine.CompileFiles(fileMap, projectInstanceContext); err != nil {
		return
	}
	return envMap, fileMap, nil
}

func (fm *ContextManager) projectEnvs() (map[string]interface{}, map[string]interface{}) {
	// project configs will be used for templating
	// prefix project configs to avoid conflicts with project/instance configs
	projectPrefixedConfig := map[string]interface{}{}
	projRawConfig := map[string]interface{}{}
	for key, val := range fm.getProjectConfigMap() {
		projectPrefixedConfig[fmt.Sprintf("%s%s", ProjectConfigPrefix, key)] = val
		projRawConfig[key] = val
	}

	// use namespace configs for templating. also, override project config with
	// namespace's configs when present
	for key, val := range fm.getNamespaceConfigMap() {
		projectPrefixedConfig[fmt.Sprintf("%s%s", ProjectConfigPrefix, key)] = val
		projRawConfig[key] = val
	}
	return projectPrefixedConfig, projRawConfig
}

func (fm *ContextManager) generateEnvs(runName string, runType models.InstanceType,
	projectInstanceContext map[string]interface{}) (map[string]string, error) {
	transformationConfigs, hookConfigs, err := fm.getConfigMaps(fm.jobRun.Spec, runName, runType)
	if err != nil {
		return nil, err
	}

	// templatize configs for transformation with project and instance
	if transformationConfigs, err = fm.compileTemplates(transformationConfigs, projectInstanceContext); err != nil {
		return nil, err
	}

	// if this is requested for transformation, just return from here
	if runType == models.InstanceTypeTask {
		return MergeInterfaceMapToString(transformationConfigs, nil), nil
	}

	// prefix transformation configs to avoid conflicts with project/instance configs
	prefixedTransformationConfigs := map[string]interface{}{}
	for k, v := range transformationConfigs {
		prefixedTransformationConfigs[fmt.Sprintf("%s%s", TaskConfigPrefix, k)] = v
	}

	// templatize configs of hook with transformation, project and instance
	projectInstanceTransformationConfigs := MergeInterfaceMapToInterface(projectInstanceContext, prefixedTransformationConfigs)
	projectInstanceTransformationConfigs["task"] = transformationConfigs
	if hookConfigs, err = fm.compileTemplates(hookConfigs, projectInstanceTransformationConfigs); err != nil {
		return nil, err
	}

	// merge transformation and hook configs
	return MergeInterfaceMapToString(prefixedTransformationConfigs, hookConfigs), nil
}

func (fm *ContextManager) compileTemplates(templateValueMap, templateContext map[string]interface{}) (map[string]interface{}, error) {
	for key, val := range templateValueMap {
		valString, ok := val.(string)
		if !ok {
			continue
		}
		compiledValue, err := fm.engine.CompileString(valString, templateContext)
		if err != nil {
			return nil, err
		}
		templateValueMap[key] = compiledValue
	}
	return templateValueMap, nil
}

func (fm *ContextManager) getProjectConfigMap() map[string]string {
	configMap := map[string]string{}
	for key, val := range fm.namespace.ProjectSpec.Config {
		configMap[key] = val
	}
	return configMap
}

func (fm *ContextManager) getNamespaceConfigMap() map[string]string {
	configMap := map[string]string{}
	for key, val := range fm.namespace.Config {
		configMap[key] = val
	}
	return configMap
}

func (fm *ContextManager) getInstanceData(instanceSpec models.InstanceSpec) (map[string]interface{}, map[string]string) {
	envMap := map[string]interface{}{}
	fileMap := map[string]string{}

	if instanceSpec.Data != nil {
		for _, jobRunData := range instanceSpec.Data {
			switch jobRunData.Type {
			case models.InstanceDataTypeFile:
				fileMap[jobRunData.Name] = jobRunData.Value
			case models.InstanceDataTypeEnv:
				envMap[jobRunData.Name] = jobRunData.Value
			}
		}
	}
	return envMap, fileMap
}

func (fm *ContextManager) getConfigMaps(jobSpec models.JobSpec, runName string,
	runType models.InstanceType) (map[string]interface{},
	map[string]interface{}, error) {
	transformationMap := map[string]interface{}{}
	for _, val := range jobSpec.Task.Config {
		transformationMap[val.Name] = val.Value
	}

	hookMap := map[string]interface{}{}
	if runType == models.InstanceTypeHook {
		hook, err := jobSpec.GetHookByName(runName)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "requested hook not found %s", runName)
		}
		for _, val := range hook.Config {
			hookMap[val.Name] = val.Value
		}
	}
	return transformationMap, hookMap, nil
}

func NewContextManager(namespace models.NamespaceSpec, jobRun models.JobRun, engine models.TemplateEngine) *ContextManager {
	return &ContextManager{
		namespace: namespace,
		jobRun:    jobRun,
		engine:    engine,
	}
}

func MergeStringMap(mp1, mp2 map[string]string) (mp3 map[string]string) {
	mp3 = make(map[string]string)
	for k, v := range mp1 {
		mp3[k] = v
	}
	for k, v := range mp2 {
		mp3[k] = v
	}
	return mp3
}

func MergeInterfaceMapToInterface(mp1, mp2 map[string]interface{}) (mp3 map[string]interface{}) {
	mp3 = make(map[string]interface{})
	for k, v := range mp1 {
		mp3[k] = v
	}
	for k, v := range mp2 {
		mp3[k] = v
	}
	return mp3
}

// merge two maps and return a map of string
// it will ignore whatever is not a string type
func MergeInterfaceMapToString(mp1, mp2 map[string]interface{}) (mp3 map[string]string) {
	mp3 = make(map[string]string)
	for k, v := range mp1 {
		if vs, ok := v.(string); ok {
			mp3[k] = vs
		}
	}
	for k, v := range mp2 {
		if vs, ok := v.(string); ok {
			mp3[k] = vs
		}
	}
	return mp3
}

// DumpAssets used for dry run and does not effect actual execution of a job
func DumpAssets(jobSpec models.JobSpec, scheduledAt time.Time, engine models.TemplateEngine, allowOverride bool) (map[string]string, error) {
	var jobDestination string
	if jobSpec.Task.Unit.DependencyMod != nil {
		jobDestinationResponse, err := jobSpec.Task.Unit.DependencyMod.GenerateDestination(context.TODO(), models.GenerateDestinationRequest{
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
		compiledAssetResponse, err := jobSpec.Task.Unit.CLIMod.CompileAssets(context.TODO(), models.CompileAssetsRequest{
			Window:           jobSpec.Task.Window,
			Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
			InstanceSchedule: scheduledAt,
			InstanceData: []models.InstanceSpecData{
				{
					Name:  ConfigKeyExecutionTime,
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
		ConfigKeyDstart:        jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
		ConfigKeyDend:          jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
		ConfigKeyExecutionTime: scheduledAt.Format(models.InstanceScheduledAtTimeLayout),
		ConfigKeyDestination:   jobDestination,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to compile templates")
	}

	return templates, nil
}
