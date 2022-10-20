package service

import (
	"errors"
	"fmt"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/service"
	"github.com/odpf/optimus/models"
	"golang.org/x/net/context"
	"time"
)

var (
	ErrDependencyModNotFound = errors.New("dependency mod not found for plugin")
)

type PluginService interface {
	GenerateDestination(context.Context, *dto.Task, *tenant.WithDetails) (string, error)
	GenerateDependencies(ctx context.Context, jobSpec *dto.JobSpec, dryRun bool) ([]string, error)
}

type JobPluginService struct {
	secretService service.SecretService

	pluginRepo models.PluginRepository
	engine     models.TemplateEngine

	Now func() time.Time
}

type AssetCompiler interface {
	Compile(ctx context.Context, jobSpec *dto.JobSpec, scheduledAt time.Time, engine models.TemplateEngine, allowOverride bool) (map[string]string, error)
}

func (p JobPluginService) GenerateDestination(ctx context.Context, task *dto.Task, tnnt *tenant.WithDetails) (string, error) {
	plugin, err := p.pluginRepo.GetByName(task.Name())
	if err != nil {
		return "", err
	}

	if plugin.DependencyMod == nil {
		return "", ErrDependencyModNotFound
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

	return destination.URN(), nil
}

func (p JobPluginService) GenerateDependencies(ctx context.Context, jobSpec *dto.JobSpec, dryRun bool) ([]string, error) {
	plugin, err := p.pluginRepo.GetByName(jobSpec.Task().Name())
	if err != nil {
		return nil, err
	}

	if plugin.DependencyMod == nil {
		return nil, ErrDependencyModNotFound
	}

	assets, err := p.compileAsset(ctx, plugin, jobSpec, p.Now(), false)
	if err != nil {
		return nil, fmt.Errorf("asset compilation failure: %w", err)
	}

	compiledConfigs, err := p.compileConfig(ctx, jobSpec.Task().Config(), jobSpec.Tenant())
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

	return resp.Dependencies, nil
}

func (p JobPluginService) compileConfig(ctx context.Context, configs *dto.Config, tnnt *tenant.WithDetails) (models.PluginConfigs, error) {
	secrets, err := p.secretService.GetAll(ctx, tnnt.ToTenant())
	if err != nil {
		return nil, err
	}

	tmplCtx := compiler.PrepareContext(
		compiler.From(tnnt.GetConfigs()).WithName("proj").WithKeyPrefix(compiler.ProjectConfigPrefix),
		compiler.From(tenant.PlainTextSecrets(secrets).ToMap()).WithName("secret"),
	)

	var pluginConfigs models.PluginConfigs
	for key, val := range configs.Config() {
		compiledConf, err := p.engine.CompileString(val, tmplCtx)
		if err != nil {
			return nil, err
		}
		pluginConfigs = append(pluginConfigs, models.PluginConfig{
			Name:  key,
			Value: compiledConf,
		})
	}
	return pluginConfigs, nil
}

func (p JobPluginService) compileAsset(ctx context.Context, plugin *models.Plugin, jobSpec *dto.JobSpec, scheduledAt time.Time, allowOverride bool) (map[string]string, error) {
	var jobDestination string
	if plugin.DependencyMod != nil {
		jobDestinationResponse, err := plugin.DependencyMod.GenerateDestination(ctx, models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromMap(jobSpec.Task().Config().Config()),
			Assets: models.PluginAssets{}.FromMap(jobSpec.Assets()),
			PluginOptions: models.PluginOptions{
				DryRun: true,
			},
		})
		if err != nil {
			return nil, err
		}
		jobDestination = jobDestinationResponse.Destination
	}

	startTime, err := jobSpec.Window().GetStartTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting start time: %w", err)
	}
	endTime, err := jobSpec.Window().GetEndTime(scheduledAt)
	if err != nil {
		return nil, fmt.Errorf("error getting end time: %w", err)
	}

	// compile again if needed
	templates, err := p.engine.CompileFiles(jobSpec.Assets(), map[string]interface{}{
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
