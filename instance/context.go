package instance

import (
	"fmt"
	"time"

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
type ContextManager struct {
	projectSpec models.ProjectSpec
	jobSpec     models.JobSpec
	engine      models.TemplateEngine
}

// Generate fetches and compiles all config data related to an instance and
// returns a map of env variables and a map[fileName]fileContent
// It compiles any templates/macros present in the config.
func (fm *ContextManager) Generate(
	instanceSpec models.InstanceSpec,
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
	instanceEnvMap, instanceFileMap := fm.getInstanceData(instanceSpec)

	// merge both
	projectInstanceContext := mergeStringMap(instanceEnvMap, projectConfig)

	// prepare configs
	envMap, err = fm.generateEnvs(runName, runType, projectInstanceContext)

	// transformation may need instance variables as well
	envMap = mergeStringMap(envMap, instanceEnvMap)
	if err != nil {
		return
	}

	// do the same for asset files
	// append job spec assets to list of files need to write
	fileMap = mergeStringMap(instanceFileMap, fm.jobSpec.Assets.ToMap())
	if fileMap, err = fm.engine.CompileFiles(fileMap, convertStringToInterfaceMap(projectInstanceContext)); err != nil {
		return
	}

	return envMap, fileMap, nil
}

func (fm *ContextManager) generateEnvs(runName string, runType models.InstanceType,
	projectInstanceContext map[string]string) (map[string]string, error) {
	transformationConfigs, hookConfigs, err := fm.getConfigMaps(fm.jobSpec, runName, runType)
	if err != nil {
		return nil, err
	}

	// templatize configs for transformation with project and instance
	if transformationConfigs, err = fm.compileTemplates(transformationConfigs, projectInstanceContext); err != nil {
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
	projectInstanceTransformationConfigs := mergeStringMap(projectInstanceContext, prefixedTransformationConfigs)
	if hookConfigs, err = fm.compileTemplates(hookConfigs, projectInstanceTransformationConfigs); err != nil {
		return nil, err
	}

	// merge transformation and hook configs
	return mergeStringMap(prefixedTransformationConfigs, hookConfigs), nil
}

func (fm *ContextManager) compileTemplates(templateValueMap, templateContext map[string]string) (map[string]string, error) {
	for key, val := range templateValueMap {
		compiledValue, err := fm.engine.CompileString(val, convertStringToInterfaceMap(templateContext))
		if err != nil {
			return nil, err
		}
		templateValueMap[key] = compiledValue
	}
	return templateValueMap, nil
}

func (fm *ContextManager) getProjectConfigMap() map[string]string {
	configMap := map[string]string{}
	for key, val := range fm.projectSpec.Config {
		configMap[key] = val
	}
	return configMap
}

func (fm *ContextManager) getInstanceData(instanceSpec models.InstanceSpec) (map[string]string, map[string]string) {
	envMap := map[string]string{}
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

func NewContextManager(projectSpec models.ProjectSpec, jobSpec models.JobSpec,
	engine models.TemplateEngine) *ContextManager {
	return &ContextManager{
		projectSpec: projectSpec,
		jobSpec:     jobSpec,
		engine:      engine,
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

func convertStringToInterfaceMap(i map[string]string) map[string]interface{} {
	o := map[string]interface{}{}
	for k, v := range i {
		o[k] = v
	}
	return o
}

func DumpAssets(jobSpec models.JobSpec, scheduledAt time.Time, engine models.TemplateEngine) (map[string]string, error) {
	jobDestination, err := jobSpec.Task.Unit.GenerateDestination(models.GenerateDestinationRequest{
		Config: jobSpec.Task.Config,
		Assets: jobSpec.Assets.ToMap(),
	})
	if err != nil {
		return nil, err
	}

	templates, err := engine.CompileFiles(jobSpec.Assets.ToMap(), map[string]interface{}{
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
