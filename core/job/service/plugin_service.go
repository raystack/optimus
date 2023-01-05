package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/compiler"
	"github.com/odpf/optimus/sdk/plugin"
)

const (
	projectConfigPrefix = "GLOBAL__"

	configKeyDstart        = "DSTART"
	configKeyDend          = "DEND"
	configKeyExecutionTime = "EXECUTION_TIME"
	configKeyDestination   = "JOB_DESTINATION"

	TimeISOFormat = time.RFC3339
)

var (
	ErrUpstreamModNotFound = errors.New("upstream mod not found for plugin")
	ErrYamlModNotExist     = errors.New("yaml mod not found for plugin")
)

type SecretsGetter interface {
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
	GetAll(ctx context.Context, projName tenant.ProjectName, namespaceName string) ([]*tenant.PlainTextSecret, error)
}

type PluginRepo interface {
	GetByName(string) (*plugin.Plugin, error)
}

type Engine interface {
	Compile(templateMap map[string]string, context map[string]any) (map[string]string, error)
	CompileString(input string, context map[string]any) (string, error)
}

type JobPluginService struct {
	secretsGetter SecretsGetter

	pluginRepo PluginRepo
	engine     Engine

	now func() time.Time

	logger log.Logger
}

func NewJobPluginService(secretsGetter SecretsGetter, pluginRepo PluginRepo, engine Engine, logger log.Logger) *JobPluginService {
	return &JobPluginService{secretsGetter: secretsGetter, pluginRepo: pluginRepo, engine: engine, logger: logger, now: time.Now}
}

func (p JobPluginService) Info(_ context.Context, taskName job.TaskName) (*plugin.Info, error) {
	taskPlugin, err := p.pluginRepo.GetByName(taskName.String())
	if err != nil {
		return nil, err
	}

	if taskPlugin.YamlMod == nil {
		return nil, ErrYamlModNotExist
	}

	return taskPlugin.YamlMod.PluginInfo(), nil
}

func (p JobPluginService) GenerateDestination(ctx context.Context, tnnt *tenant.WithDetails, task job.Task) (job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(task.Name().String())
	if err != nil {
		return "", err
	}

	if taskPlugin.DependencyMod == nil {
		return "", ErrUpstreamModNotFound
	}

	compiledConfig, err := p.compileConfig(ctx, task.Config().Map(), tnnt)
	if err != nil {
		return "", err
	}

	destination, err := taskPlugin.DependencyMod.GenerateDestination(ctx, plugin.GenerateDestinationRequest{
		Config: compiledConfig,
	})

	if err != nil {
		return "", fmt.Errorf("failed to generate destination: %w", err)
	}

	return job.ResourceURN(destination.URN()), nil
}

func (p JobPluginService) GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]job.ResourceURN, error) {
	taskPlugin, err := p.pluginRepo.GetByName(spec.Task().Name().String())
	if err != nil {
		return nil, err
	}

	if taskPlugin.DependencyMod == nil {
		return nil, ErrUpstreamModNotFound
	}

	// TODO: this now will always be a same time for start of service, is it correct ?
	assets, err := p.compileAsset(ctx, taskPlugin, spec, p.now())
	if err != nil {
		return nil, fmt.Errorf("asset compilation failure: %w", err)
	}

	compiledConfigs, err := p.compileConfig(ctx, spec.Task().Config(), jobTenant)
	if err != nil {
		return nil, err
	}

	resp, err := taskPlugin.DependencyMod.GenerateDependencies(ctx, plugin.GenerateDependenciesRequest{
		Config: compiledConfigs,
		Assets: plugin.AssetsFromMap(assets),
		Options: plugin.Options{
			DryRun: dryRun,
		},
	})
	if err != nil {
		return nil, err
	}

	var upstreamURNs []job.ResourceURN
	for _, dependency := range resp.Dependencies {
		resourceURN := job.ResourceURN(dependency)
		upstreamURNs = append(upstreamURNs, resourceURN)
	}

	return upstreamURNs, nil
}

func (p JobPluginService) compileConfig(ctx context.Context, configs job.Config, tnnt *tenant.WithDetails) (plugin.Configs, error) {
	jobTenant := tnnt.ToTenant()
	secrets, err := p.secretsGetter.GetAll(ctx, jobTenant.ProjectName(), jobTenant.NamespaceName().String())
	if err != nil {
		return nil, err
	}

	tmplCtx := compiler.PrepareContext(
		compiler.From(tnnt.GetConfigs()).WithName("proj").WithKeyPrefix(projectConfigPrefix),
		compiler.From(tenant.PlainTextSecrets(secrets).ToMap()).WithName("secret"),
	)

	var pluginConfigs plugin.Configs
	for key, val := range configs {
		compiledConf, err := p.engine.CompileString(val, tmplCtx)
		if err != nil {
			p.logger.Warn("error in template compilation: ", err.Error())
		}
		pluginConfigs = append(pluginConfigs, plugin.Config{
			Name:  key,
			Value: compiledConf,
		})
	}
	return pluginConfigs, nil
}

func (p JobPluginService) compileAsset(ctx context.Context, taskPlugin *plugin.Plugin, spec *job.Spec, scheduledAt time.Time) (map[string]string, error) {
	var jobDestination string
	if taskPlugin.DependencyMod != nil {
		var assets map[string]string
		if spec.Asset() != nil {
			assets = spec.Asset()
		}
		jobDestinationResponse, err := taskPlugin.DependencyMod.GenerateDestination(ctx, plugin.GenerateDestinationRequest{
			Config: plugin.ConfigsFromMap(spec.Task().Config()),
			Assets: plugin.AssetsFromMap(assets),
			Options: plugin.Options{
				DryRun: true,
			},
		})
		if err != nil {
			return nil, err
		}
		jobDestination = jobDestinationResponse.Destination
	}

	startTime, err := spec.Window().GetStartTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting start time: %w", err)
	}
	endTime, err := spec.Window().GetEndTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting end time: %w", err)
	}

	var assets map[string]string
	if spec.Asset() != nil {
		assets = spec.Asset()
	}

	templates, err := p.engine.Compile(assets, map[string]interface{}{
		configKeyDstart:        startTime.Format(TimeISOFormat),
		configKeyDend:          endTime.Format(TimeISOFormat),
		configKeyExecutionTime: scheduledAt.Format(TimeISOFormat),
		configKeyDestination:   jobDestination,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to compile templates: %w", err)
	}

	return templates, nil
}
