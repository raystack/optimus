package instance

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

const (
	// TaskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	TaskConfigPrefix = "TASK__"

	// ProjectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	ProjectConfigPrefix = "GLOBAL__"
)

// TODO: think of a better name
// FeatureManager fetches all config data for a given instanceSpec and compiles all
// macros/templates.
// Feature here is a term used for the input required for tasks to execute.
// Raw task assets may not be executable in there default state and needs to be
// transformed before they can work as inputs. Input could be through
// environment variables or as a file.
type FeatureManager struct {
	projectSpec  models.ProjectSpec
	jobSpec      models.JobSpec
	instanceSpec models.InstanceSpec
}

// Generate fetches and compiles all config data related to an instance and
// returns a map of env variables and a map[fileName]fileContent
// It compiles any templates/macros present in the config.
func (fm *FeatureManager) Generate(
	runType models.InstanceType,
	runName string,
) (envMap map[string]string, fileMap map[string]string, err error) {
	envMap = make(map[string]string)
	fileMap = make(map[string]string)

	// project configs will be used for templating
	// prefix project configs to avoid conflicts with project/instance configs
	projectConfig := map[string]string{}
	for key, val := range fm.getProjectConfigMap() {
		projectConfig[fmt.Sprintf("%s%s", ProjectConfigPrefix, key)] = val
	}

	// instance env will be used for templating
	instanceEnvMap, instanceFileMap := fm.getInstanceData()

	// merge both
	projectInstanceConfigs := mergeStringMap(instanceEnvMap, projectConfig)

	// prepare configs
	envMap, err = fm.generateEnvs(runName, runType, projectInstanceConfigs)

	// transformation may need instance variables as well
	envMap = mergeStringMap(envMap, instanceEnvMap)
	if err != nil {
		return
	}

	// do the same for asset files
	// append job spec assets to list of files need to write
	fileMap = mergeStringMap(instanceFileMap, fm.jobSpec.Assets.ToMap())
	if fileMap, err = fm.compileTemplates(projectInstanceConfigs, fileMap); err != nil {
		return
	}

	return envMap, fileMap, nil
}

func (fm *FeatureManager) generateEnvs(runName string, runType models.InstanceType,
	projectInstanceConfigs map[string]string) (map[string]string, error) {
	transformationConfigs, hookConfigs, err := fm.getConfigMaps(fm.jobSpec, runName, runType)
	if err != nil {
		return nil, err
	}

	// templatize configs for transformation with project and instance
	if transformationConfigs, err = fm.compileTemplates(projectInstanceConfigs, transformationConfigs); err != nil {
		return nil, err
	}

	// if this is requested for transformation, just return from here
	if runType == models.InstanceTypeTransformation {
		return transformationConfigs, nil
	}

	// prefix transformation configs to avoid conflicts with project/instance configs
	prefixedTransformationConfigs := map[string]string{}
	for key, val := range transformationConfigs {
		prefixedTransformationConfigs[fmt.Sprintf("%s%s", TaskConfigPrefix, key)] = val
	}

	// templatize configs of hook with transformation, project and instance
	projectInstanceTransformationConfigs := mergeStringMap(projectInstanceConfigs, prefixedTransformationConfigs)
	if hookConfigs, err = fm.compileTemplates(projectInstanceTransformationConfigs, hookConfigs); err != nil {
		return nil, err
	}

	// merge transformation and hook configs
	return mergeStringMap(prefixedTransformationConfigs, hookConfigs), nil
}

func (fm *FeatureManager) compileTemplates(templateValues, templateMap map[string]string) (map[string]string, error) {
	compiler := NewMacroCompiler()
	for key, val := range templateMap {
		compiledValue, err := compiler.CompileTemplate(templateValues, val)
		if err != nil {
			return nil, err
		}
		templateMap[key] = compiledValue
	}
	return templateMap, nil
}

func (fm *FeatureManager) getAssetFilesMap(jobSpec models.JobSpec, fileMap map[string]string) map[string]string {
	return jobSpec.Assets.ToMap()
	for _, jobAsset := range jobSpec.Assets.GetAll() {
		fileMap[jobAsset.Name] = jobAsset.Value
	}
	return fileMap
}

func (fm *FeatureManager) getProjectConfigMap() map[string]string {
	configMap := map[string]string{}
	for key, val := range fm.projectSpec.Config {
		configMap[key] = val
	}
	return configMap
}

func (fm *FeatureManager) getInstanceData() (map[string]string, map[string]string) {
	envMap := map[string]string{}
	fileMap := map[string]string{}

	if fm.instanceSpec.Data != nil {
		for _, jobRunData := range fm.instanceSpec.Data {
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

func (fm *FeatureManager) getConfigMaps(jobSpec models.JobSpec, runName string,
	runType models.InstanceType) (map[string]string,
	map[string]string, error) {
	transformationMap := map[string]string{}
	for _, val := range jobSpec.Task.Config {
		transformationMap[val.Name] = val.Value
	}

	hookMap := map[string]string{}
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

func NewFeatureManager(projectSpec models.ProjectSpec, jobSpec models.JobSpec, instanceSpec models.InstanceSpec) *FeatureManager {
	return &FeatureManager{
		projectSpec:  projectSpec,
		jobSpec:      jobSpec,
		instanceSpec: instanceSpec,
	}
}

func mergeStringMap(mp1, mp2 map[string]string) (mp3 map[string]string) {
	mp3 = make(map[string]string)
	for k, v := range mp1 {
		mp3[k] = v
	}
	for k, v := range mp2 {
		mp3[k] = v
	}
	return mp3
}
