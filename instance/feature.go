package instance

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

// FeatureManager fetches all config data for a given instanceSpecinstance and compiles all
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
	taskType string,
	taskName string,
) (map[string]string, map[string]string, error) {
	envMap, fileMap := fm.getInstanceData()

	// append job spec assets to list of files needs to write
	for _, jobAsset := range fm.jobSpec.Assets.GetAll() {
		fileMap[jobAsset.Name] = jobAsset.Value
	}

	envMap, err := fm.getExecUnitConfigMap(fm.jobSpec, taskType, taskName, envMap)
	if err != nil {
		return nil, nil, err
	}

	// now compile all the values in the map.
	// while compiling we'll also include the config defined at the projectSpec level
	templateValues := fm.getProjectConfigMap()
	for k, v := range envMap {
		templateValues[k] = v
	}
	if envMap, err = fm.compileTemplates(templateValues, envMap); err != nil {
		return nil, nil, err
	}
	if fileMap, err = fm.compileTemplates(templateValues, fileMap); err != nil {
		return nil, nil, err
	}

	return envMap, fileMap, nil
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

func (fm *FeatureManager) getExecUnitConfigMap(jobSpec models.JobSpec, taskType string, taskName string,
	envMap map[string]string) (map[string]string, error) {

	if taskType == models.InstanceTypeTransformation {
		for key, val := range jobSpec.Task.Config {
			envMap[key] = val
		}
	} else if taskType == models.InstanceTypeHook {
		hook, err := jobSpec.GetHookByName(taskName)
		if err != nil {
			return nil, errors.Wrapf(err, "requested hook not found %s", taskName)
		}
		for key, val := range hook.Config {
			envMap[key] = val
		}
	}
	return envMap, nil
}

func NewFeatureManager(projectSpec models.ProjectSpec, jobSpec models.JobSpec, instanceSpec models.InstanceSpec) *FeatureManager {
	return &FeatureManager{
		projectSpec: projectSpec,
		jobSpec: jobSpec,
		instanceSpec: instanceSpec,
	}
}
