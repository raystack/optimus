package service

import (
	"context"
	"strings"
	"time"

	"github.com/raystack/salt/log"

	"github.com/raystack/optimus/core/scheduler"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/internal/compiler"
	"github.com/raystack/optimus/internal/utils"
)

const (
	// taskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	taskConfigPrefix = "TASK__"

	// projectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	projectConfigPrefix = "GLOBAL__"

	contextProject       = "proj"
	contextSecret        = "secret"
	contextSystemDefined = "inst"
	contextTask          = "task"

	SecretsStringToMatch = ".secret."

	TimeISOFormat = time.RFC3339

	// Configuration for system defined variables
	configDstart        = "DSTART"
	configDend          = "DEND"
	configExecutionTime = "EXECUTION_TIME"
	configDestination   = "JOB_DESTINATION"
)

type TenantService interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
	GetSecrets(ctx context.Context, tnnt tenant.Tenant) ([]*tenant.PlainTextSecret, error)
}

type TemplateCompiler interface {
	Compile(templateMap map[string]string, context map[string]any) (map[string]string, error)
}

type AssetCompiler interface {
	CompileJobRunAssets(ctx context.Context, job *scheduler.Job, systemEnvVars map[string]string, scheduledAt time.Time, contextForTask map[string]interface{}) (map[string]string, error)
}

type InputCompiler struct {
	tenantService TenantService
	compiler      TemplateCompiler
	assetCompiler AssetCompiler

	logger log.Logger
}

func (i InputCompiler) Compile(ctx context.Context, job *scheduler.Job, config scheduler.RunConfig, executedAt time.Time) (*scheduler.ExecutorInput, error) {
	tenantDetails, err := i.tenantService.GetDetails(ctx, job.Tenant)
	if err != nil {
		i.logger.Error("error getting tenant details: %s", err)
		return nil, err
	}

	systemDefinedVars, err := getSystemDefinedConfigs(job, config, executedAt)
	if err != nil {
		i.logger.Error("error getting config for job [%s]: %s", job.Name.String(), err)
		return nil, err
	}

	// Prepare template context and compile task config
	taskContext := compiler.PrepareContext(
		compiler.From(tenantDetails.GetConfigs()).WithName(contextProject).WithKeyPrefix(projectConfigPrefix),
		compiler.From(tenantDetails.SecretsMap()).WithName(contextSecret),
		compiler.From(systemDefinedVars).WithName(contextSystemDefined).AddToContext(),
	)

	// Compile asset files
	fileMap, err := i.assetCompiler.CompileJobRunAssets(ctx, job, systemDefinedVars, config.ScheduledAt, taskContext)
	if err != nil {
		i.logger.Error("error compiling job run assets: %s", err)
		return nil, err
	}

	confs, secretConfs, err := i.compileConfigs(job.Task.Config, taskContext)
	if err != nil {
		i.logger.Error("error compiling task config: %s", err)
		return nil, err
	}

	if config.Executor.Type == scheduler.ExecutorTask {
		return &scheduler.ExecutorInput{
			Configs: utils.MergeMaps(confs, systemDefinedVars),
			Secrets: secretConfs,
			Files:   fileMap,
		}, nil
	}

	// If request for hook, add task configs to templateContext
	hookContext := compiler.PrepareContext(
		compiler.From(confs, secretConfs).WithName(contextTask).WithKeyPrefix(taskConfigPrefix),
	)

	mergedContext := utils.MergeAnyMaps(taskContext, hookContext)

	hook, err := job.GetHook(config.Executor.Name)
	if err != nil {
		i.logger.Error("error getting hook [%s]: %s", config.Executor.Name, err)
		return nil, err
	}

	hookConfs, hookSecrets, err := i.compileConfigs(hook.Config, mergedContext)
	if err != nil {
		i.logger.Error("error compiling configs for hook [%s]: %s", hook.Name, err)
		return nil, err
	}

	return &scheduler.ExecutorInput{
		Configs: utils.MergeMaps(hookConfs, systemDefinedVars),
		Secrets: hookSecrets,
		Files:   fileMap,
	}, nil
}

func (i InputCompiler) compileConfigs(configs map[string]string, templateCtx map[string]any) (map[string]string, map[string]string, error) {
	conf, secretsConfig := splitConfigWithSecrets(configs)

	var err error
	if conf, err = i.compiler.Compile(conf, templateCtx); err != nil {
		i.logger.Error("error compiling template with config: %s", err)
		return nil, nil, err
	}

	if secretsConfig, err = i.compiler.Compile(secretsConfig, templateCtx); err != nil {
		i.logger.Error("error compiling template with secret: %s", err)
		return nil, nil, err
	}

	return conf, secretsConfig, nil
}

func getSystemDefinedConfigs(job *scheduler.Job, runConfig scheduler.RunConfig, executedAt time.Time) (map[string]string, error) {
	startTime, err := job.Window.GetStartTime(runConfig.ScheduledAt)
	if err != nil {
		return nil, err
	}
	endTime, err := job.Window.GetEndTime(runConfig.ScheduledAt)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		configDstart:        startTime.Format(TimeISOFormat),
		configDend:          endTime.Format(TimeISOFormat),
		configExecutionTime: executedAt.Format(TimeISOFormat),
		configDestination:   job.Destination,
	}, nil
}

func splitConfigWithSecrets(conf map[string]string) (map[string]string, map[string]string) {
	configs := map[string]string{}
	configWithSecrets := map[string]string{}
	for name, val := range conf {
		if strings.Contains(val, SecretsStringToMatch) {
			configWithSecrets[name] = val
			continue
		}
		configs[name] = val
	}

	return configs, configWithSecrets
}

func NewJobInputCompiler(tenantService TenantService, compiler TemplateCompiler, assetCompiler AssetCompiler, logger log.Logger) *InputCompiler {
	return &InputCompiler{
		tenantService: tenantService,
		compiler:      compiler,
		assetCompiler: assetCompiler,
		logger:        logger,
	}
}
