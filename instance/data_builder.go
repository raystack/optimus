package instance

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

// DataBuilder fetches all config data for a given instance and compiles all macros/templates
type DataBuilder struct {
}

// GetData fetches and compiles all config data related to an instance and
// returns a map of env variables and a map[fileName]fileContent
// It compiles any templates/macros present in the config.
func (dataBuilder *DataBuilder) GetData(
	project models.ProjectSpec,
	jobSpec models.JobSpec,
	instance models.InstanceSpec,
	instanceType string,
	execUnitName string,
) (map[string]string, map[string]string, error) {

	envMap, fileMap := dataBuilder.getInstanceData(instance)
	fileMap = dataBuilder.getAssetFilesMap(jobSpec, fileMap)
	envMap, err := dataBuilder.getExecUnitConfigMap(jobSpec, instanceType, execUnitName, envMap)
	if err != nil {
		return nil, nil, err
	}

	// now compile all the values in the map.
	// while compiling we'll also include the config defined at the project level
	templateValues := dataBuilder.getProjectConfigMap(project)
	for k, v := range envMap {
		templateValues[k] = v
	}
	if envMap, err = dataBuilder.compileTemplates(templateValues, envMap); err != nil {
		return nil, nil, err
	}
	if fileMap, err = dataBuilder.compileTemplates(templateValues, fileMap); err != nil {
		return nil, nil, err
	}

	return envMap, fileMap, nil
}

func (dataBuilder *DataBuilder) compileTemplates(templateValues, templateMap map[string]string) (map[string]string, error) {
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

func (dataBuilder *DataBuilder) getAssetFilesMap(jobSpec models.JobSpec, fileMap map[string]string) map[string]string {
	for _, jobAsset := range jobSpec.Assets.GetAll() {
		fileMap[jobAsset.Name] = jobAsset.Value
	}
	return fileMap
}

func (dataBuilder *DataBuilder) getProjectConfigMap(project models.ProjectSpec) map[string]string {
	configMap := map[string]string{}
	for key, val := range project.Config {
		configMap[key] = val
	}
	return configMap
}

func (dataBuilder *DataBuilder) getInstanceData(instance models.InstanceSpec) (map[string]string, map[string]string) {
	envMap := map[string]string{}
	fileMap := map[string]string{}

	if instance.Data != nil {
		for _, jobRunData := range instance.Data {
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

func (dataBuilder *DataBuilder) getExecUnitConfigMap(jobSpec models.JobSpec, instanceType string, runName string,
	envMap map[string]string) (map[string]string, error) {

	if instanceType == models.InstanceTypeTransformation {
		for key, val := range jobSpec.Task.Config {
			envMap[key] = val
		}
	} else if instanceType == models.InstanceTypeHook {
		hook, err := jobSpec.GetHookByName(runName)
		if err != nil {
			return nil, errors.Wrapf(err, "requested hook not found %s", runName)
		}
		for key, val := range hook.Config {
			envMap[key] = val
		}
	}
	return envMap, nil
}

func NewDataBuilder() *DataBuilder {
	return &DataBuilder{}
}
