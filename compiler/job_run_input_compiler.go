package compiler

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/utils"
)

const (
	// TaskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	TaskConfigPrefix = "TASK__"

	// ProjectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	ProjectConfigPrefix = "GLOBAL__"
)

type JobRunInputCompiler interface {
	// Compile prepares instance execution context environment
	Compile(ctx context.Context, namespaceSpec models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (assets *models.JobRunInput, err error)
}

type compiler struct {
	secretService  service.SecretService
	configCompiler *JobConfigCompiler
	assetsCompiler *JobRunAssetsCompiler
}

func (c compiler) Compile(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (
	*models.JobRunInput, error) {
	secrets, err := c.getSecretsMap(ctx, namespace)
	if err != nil {
		return nil, err
	}
	instanceConfig := getInstanceEnv(instanceSpec)

	// Prepare template context and compile task config
	taskContext := PrepareContext(
		From(namespace.ProjectSpec.Config, namespace.Config).WithName("proj").WithKeyPrefix(ProjectConfigPrefix),
		From(secrets).WithName("secret"),
		From(instanceConfig).WithName("inst").AddToContext(),
	)

	taskCompiledConfs, err := c.configCompiler.CompileConfigs(jobRun.Spec.Task.Config, taskContext)
	if err != nil {
		return nil, err
	}

	// Compile the assets using context for task
	fileMap, err := c.assetsCompiler.CompileJobRunAssets(jobRun, instanceSpec, taskContext)
	if err != nil {
		return nil, err
	}

	// If task request return the compiled assets and config
	if instanceSpec.Type == models.InstanceTypeTask {
		return &models.JobRunInput{
			ConfigMap:  utils.MergeMaps(taskCompiledConfs.Configs, instanceConfig),
			SecretsMap: taskCompiledConfs.Secrets,
			FileMap:    fileMap,
		}, nil
	}

	// If request for hook, add task configs to templateContext
	hookContext := PrepareContext(
		From(taskCompiledConfs.Configs, taskCompiledConfs.Secrets).WithName("task").WithKeyPrefix(TaskConfigPrefix),
	)
	mergedContext := utils.MergeAnyMaps(taskContext, hookContext)
	hookConfs, err := c.compileConfigForHook(instanceSpec.Name, jobRun, mergedContext)
	if err != nil {
		return nil, err
	}

	return &models.JobRunInput{
		ConfigMap:  utils.MergeMaps(hookConfs.Configs, instanceConfig),
		SecretsMap: hookConfs.Secrets,
		FileMap:    fileMap,
	}, nil
}

func (c compiler) compileConfigForHook(hookName string, jobRun models.JobRun, templateContext map[string]interface{}) (*CompiledConfigs, error) {
	hook, err := jobRun.Spec.GetHookByName(hookName)
	if err != nil {
		return nil, fmt.Errorf("requested hook not found %s: %w", hookName, err)
	}

	hookConfs, err := c.configCompiler.CompileConfigs(hook.Config, templateContext)
	if err != nil {
		return nil, err
	}

	return hookConfs, err
}

func (c compiler) getSecretsMap(ctx context.Context, namespace models.NamespaceSpec) (map[string]string, error) {
	secrets, err := c.secretService.GetSecrets(ctx, namespace)
	if err != nil {
		return nil, err
	}

	return models.ProjectSecrets(secrets).ToMap(), nil
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

func NewJobRunInputCompiler(secretService service.SecretService, confComp *JobConfigCompiler, assetCompiler *JobRunAssetsCompiler) *compiler {
	return &compiler{
		secretService:  secretService,
		configCompiler: confComp,
		assetsCompiler: assetCompiler,
	}
}
