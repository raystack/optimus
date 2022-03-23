package run

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

const (
	// TaskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	TaskConfigPrefix = "TASK__"

	// ProjectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	ProjectConfigPrefix = "GLOBAL__"

	SecretsStringToMatch = ".secret."
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
	secrets   models.ProjectSecrets
	jobRun    models.JobRun
	engine    models.TemplateEngine
}

// Generate fetches and compiles all config data related to an instance and
// returns jobRunInput, containing config required for running the job.
// It compiles any templates/macros present in the config.
func (fm *ContextManager) Generate(instanceSpec models.InstanceSpec) (*models.JobRunInput, error) {
	var configMap map[string]string
	var secretConfigs map[string]string

	instanceConfig := getInstanceEnv(instanceSpec)
	contextForTask := fm.createContextForTask(instanceConfig)
	taskConfigs, taskSecretConfigs, err := fm.compileTaskConfigs(contextForTask)
	if err != nil {
		return nil, err
	}

	configMap = taskConfigs
	secretConfigs = taskSecretConfigs

	if instanceSpec.Type == models.InstanceTypeHook {
		contextForHook := fm.createContextForHook(contextForTask, taskConfigs, taskSecretConfigs)
		hookConfigs, hookSecretConfigs, err := fm.compileHookConfigs(instanceSpec.Name, contextForHook)
		if err != nil {
			return nil, err
		}
		// Removed prefixedTaskConfig from configMap for hook, remove comment if no issues found in qa
		configMap = hookConfigs
		secretConfigs = hookSecretConfigs
	}

	fileMap, err := fm.constructCompiledFileMap(instanceSpec, contextForTask)
	if err != nil {
		return nil, err
	}
	return &models.JobRunInput{
		ConfigMap:  utils.MergeMaps(configMap, instanceConfig),
		SecretsMap: secretConfigs,
		FileMap:    fileMap,
	}, nil
}

func (fm *ContextManager) createContextForTask(instanceConfig map[string]string) map[string]interface{} {
	contextForTask := map[string]interface{}{}

	// Collect project config
	projectConfig := fm.collectProjectConfigs()
	contextForTask["proj"] = projectConfig
	utils.AppendToMap(contextForTask, prefixKeysOf(projectConfig, ProjectConfigPrefix))

	// Collect secrets
	secretMap := getSecretsMap(fm.secrets)
	contextForTask["secret"] = secretMap

	// Collect instance config for templating
	utils.AppendToMap(contextForTask, instanceConfig)
	contextForTask["inst"] = instanceConfig
	return contextForTask
}

func (fm *ContextManager) createContextForHook(initialContext map[string]interface{}, taskConfigs map[string]string, taskSecretConfigs map[string]string) map[string]interface{} {
	// Merge taskConfig and secret config for the context
	mergedTaskConfigs := utils.MergeMaps(taskConfigs, taskSecretConfigs)

	hookContext := utils.MergeAnyMaps(initialContext)
	hookContext["task"] = mergedTaskConfigs
	utils.AppendToMap(hookContext, prefixKeysOf(mergedTaskConfigs, TaskConfigPrefix))

	return hookContext
}

func (fm *ContextManager) constructCompiledFileMap(instanceSpec models.InstanceSpec, contextForTask map[string]interface{}) (map[string]string, error) {
	// check if task needs to override the compilation behaviour
	compiledAssetResponse, err := fm.jobRun.Spec.Task.Unit.CLIMod.CompileAssets(context.Background(), models.CompileAssetsRequest{
		Window:           fm.jobRun.Spec.Task.Window,
		Config:           models.PluginConfigs{}.FromJobSpec(fm.jobRun.Spec.Task.Config),
		Assets:           models.PluginAssets{}.FromJobSpec(fm.jobRun.Spec.Assets),
		InstanceSchedule: fm.jobRun.ScheduledAt,
		InstanceData:     instanceSpec.Data,
	})
	if err != nil {
		return nil, err
	}

	var fileMap map[string]string
	instanceFileMap := getInstanceFiles(instanceSpec)
	fileMap = utils.MergeMaps(instanceFileMap, compiledAssetResponse.Assets.ToJobSpec().ToMap())
	if fileMap, err = fm.engine.CompileFiles(fileMap, contextForTask); err != nil {
		return nil, err
	}
	return fileMap, nil
}

func (fm *ContextManager) collectProjectConfigs() map[string]string {
	// project configs will be used for templating
	// override project config with namespace's configs when present
	return utils.MergeMaps(fm.namespace.ProjectSpec.Config, fm.namespace.Config)
}

func (fm *ContextManager) compileConfigs(config models.JobSpecConfigs, ctx map[string]interface{}) (map[string]string, map[string]string, error) {
	conf, secretsConfig := splitConfigForSecrets(config)

	var err error
	if conf, err = fm.compileTemplates(conf, ctx); err != nil {
		return nil, nil, err
	}

	if secretsConfig, err = fm.compileTemplates(secretsConfig, ctx); err != nil {
		return nil, nil, err
	}

	return conf, secretsConfig, nil
}

func (fm *ContextManager) compileTaskConfigs(ctx map[string]interface{}) (map[string]string, map[string]string, error) {
	return fm.compileConfigs(fm.jobRun.Spec.Task.Config, ctx)
}

func (fm *ContextManager) compileHookConfigs(hookName string, templateContext map[string]interface{}) (
	map[string]string, map[string]string, error) {
	hook, err := fm.jobRun.Spec.GetHookByName(hookName)
	if err != nil {
		return nil, nil, fmt.Errorf("requested hook not found %s: %w", hookName, err)
	}

	hookConfigs, withSecrets, err := fm.compileConfigs(hook.Config, templateContext)
	if err != nil {
		return nil, nil, err
	}

	return hookConfigs, withSecrets, nil
}

func (fm *ContextManager) compileTemplates(templateValueMap map[string]string, templateContext map[string]interface{}) (map[string]string, error) {
	for key, val := range templateValueMap {
		compiledValue, err := fm.engine.CompileString(val, templateContext)
		if err != nil {
			return nil, err
		}
		templateValueMap[key] = compiledValue
	}
	return templateValueMap, nil
}

func getSecretsMap(secrets models.ProjectSecrets) map[string]string {
	secretsMap := map[string]string{}

	for _, s := range secrets {
		secretsMap[s.Name] = s.Value
	}
	return secretsMap
}

func splitConfigForSecrets(jobSpecConfig models.JobSpecConfigs) (map[string]string, map[string]string) {
	configs := map[string]string{}
	configWithSecrets := map[string]string{}
	for _, val := range jobSpecConfig {
		if strings.Contains(val.Value, SecretsStringToMatch) {
			configWithSecrets[val.Name] = val.Value
			continue
		}
		configs[val.Name] = val.Value
	}

	return configs, configWithSecrets
}

func prefixKeysOf(configMap map[string]string, prefix string) map[string]string {
	prefixedConfig := map[string]string{}
	for key, val := range configMap {
		prefixedConfig[fmt.Sprintf("%s%s", prefix, key)] = val
	}
	return prefixedConfig
}

func getInstanceEnv(instanceSpec models.InstanceSpec) map[string]string {
	if instanceSpec.Data == nil {
		return nil
	}
	envMap := map[string]string{}
	for _, jobRunData := range instanceSpec.Data {
		if jobRunData.Type == models.InstanceDataTypeEnv {
			envMap[jobRunData.Name] = jobRunData.Value
		}
	}
	return envMap
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

func NewContextManager(namespace models.NamespaceSpec, secrets models.ProjectSecrets, jobRun models.JobRun, engine models.TemplateEngine) *ContextManager {
	return &ContextManager{
		namespace: namespace,
		secrets:   secrets,
		jobRun:    jobRun,
		engine:    engine,
	}
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
		return nil, fmt.Errorf("failed to compile templates: %w", err)
	}

	return templates, nil
}
