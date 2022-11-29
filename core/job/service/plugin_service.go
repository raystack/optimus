package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/models"
)

var (
	ErrUpstreamModNotFound = errors.New("upstream mod not found for plugin")
	ErrYamlModNotExist     = errors.New("yaml mod not found for plugin")
)

type SecretsGetter interface {
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
	GetAll(ctx context.Context, projName tenant.ProjectName, namespaceName string) ([]*tenant.PlainTextSecret, error)
}

type JobPluginService struct {
	secretsGetter SecretsGetter

	pluginRepo models.PluginRepository
	engine     models.TemplateEngine

	now func() time.Time

	logger log.Logger
}

func NewJobPluginService(secretsGetter SecretsGetter, pluginRepo models.PluginRepository, engine models.TemplateEngine, logger log.Logger) *JobPluginService {
	return &JobPluginService{secretsGetter: secretsGetter, pluginRepo: pluginRepo, engine: engine, logger: logger, now: time.Now}
}

func (p JobPluginService) Info(ctx context.Context, task *job.Task) (*models.PluginInfoResponse, error) {
	plugin, err := p.pluginRepo.GetByName(task.Name().String())
	if err != nil {
		return nil, err
	}

	if plugin.YamlMod == nil {
		return nil, ErrYamlModNotExist
	}

	return plugin.YamlMod.PluginInfo(), nil
}

func (p JobPluginService) GenerateDestination(ctx context.Context, tnnt *tenant.WithDetails, task *job.Task) (job.ResourceURN, error) {
	plugin, err := p.pluginRepo.GetByName(task.Name().String())
	if err != nil {
		return "", err
	}

	if plugin.DependencyMod == nil {
		return "", ErrUpstreamModNotFound
	}

	compiledConfig, err := p.compileConfig(ctx, task.Config(), tnnt)
	if err != nil {
		return "", err
	}

	destination, err := plugin.DependencyMod.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config: compiledConfig,
	})

	if err != nil {
		return "", fmt.Errorf("failed to generate destination: %w", err)
	}

	destinationURN := job.ResourceURN(destination.URN())
	if err != nil {
		return "", err
	}
	return destinationURN, nil
}

func (p JobPluginService) GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]job.ResourceURN, error) {
	plugin, err := p.pluginRepo.GetByName(spec.Task().Name().String())
	if err != nil {
		return nil, err
	}

	if plugin.DependencyMod == nil {
		return nil, ErrUpstreamModNotFound
	}

	assets, err := p.compileAsset(ctx, plugin, spec, p.now())
	if err != nil {
		return nil, fmt.Errorf("asset compilation failure: %w", err)
	}

	compiledConfigs, err := p.compileConfig(ctx, spec.Task().Config(), jobTenant)
	if err != nil {
		return nil, err
	}

	resp, err := plugin.DependencyMod.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
		Config: compiledConfigs,
		Assets: models.PluginAssets{}.FromMap(assets),
		PluginOptions: models.PluginOptions{
			DryRun: dryRun,
		},
	})
	if err != nil {
		return nil, err
	}

	var upstreamURNs []job.ResourceURN
	for _, dependency := range resp.Dependencies {
		resourceURN := job.ResourceURN(dependency)
		if err != nil {
			return nil, err
		}
		upstreamURNs = append(upstreamURNs, resourceURN)
	}

	return upstreamURNs, nil
}

func (p JobPluginService) compileConfig(ctx context.Context, configs *job.Config, tnnt *tenant.WithDetails) (models.PluginConfigs, error) {
	jobTenant := tnnt.ToTenant()
	secrets, err := p.secretsGetter.GetAll(ctx, jobTenant.ProjectName(), jobTenant.NamespaceName().String())
	if err != nil {
		return nil, err
	}

	tmplCtx := compiler.PrepareContext(
		compiler.From(tnnt.GetConfigs()).WithName("proj").WithKeyPrefix(compiler.ProjectConfigPrefix),
		compiler.From(tenant.PlainTextSecrets(secrets).ToMap()).WithName("secret"),
	)

	var pluginConfigs models.PluginConfigs
	for key, val := range configs.Configs() {
		compiledConf, err := p.engine.CompileString(val, tmplCtx)
		if err != nil {
			p.logger.Warn("error in template compilation: ", err.Error())
		}
		pluginConfigs = append(pluginConfigs, models.PluginConfig{
			Name:  key,
			Value: compiledConf,
		})
	}
	return pluginConfigs, nil
}

func (p JobPluginService) compileAsset(ctx context.Context, plugin *models.Plugin, spec *job.Spec, scheduledAt time.Time) (map[string]string, error) {
	var jobDestination string
	if plugin.DependencyMod != nil {
		var assets map[string]string
		if spec.Asset() != nil {
			assets = spec.Asset().Assets()
		}
		jobDestinationResponse, err := plugin.DependencyMod.GenerateDestination(ctx, models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromMap(spec.Task().Config().Configs()),
			Assets: models.PluginAssets{}.FromMap(assets),
			PluginOptions: models.PluginOptions{
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
		assets = spec.Asset().Assets()
	}

	templates, err := p.engine.CompileFiles(assets, map[string]interface{}{
		models.ConfigKeyDstart:        startTime.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyDend:          endTime.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyExecutionTime: scheduledAt.Format(models.InstanceScheduledAtTimeLayout),
		models.ConfigKeyDestination:   jobDestination,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to compile templates: %w", err)
	}

	return templates, nil
}
