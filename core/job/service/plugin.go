package service

import (
	"errors"
	"fmt"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/core/tenant/service"
	"github.com/odpf/optimus/models"
	"golang.org/x/net/context"
)

var (
	ErrDependencyModNotFound = errors.New("dependency mod not found for plugin")
)

type PluginService interface {
	GenerateDestination(context.Context, *dto.Task, *tenant.WithDetails) (job.Destination, error)
	GenerateDependencies(ctx context.Context, jobSpec *dto.JobSpec, dryRun bool) ([]job.Source, error)
}

type JobPluginService struct {
	secretService service.SecretService

	// to be updated
	pluginRepo models.PluginRepository
	engine     models.TemplateEngine
}

func (p JobPluginService) GenerateDestination(ctx context.Context, task *dto.Task, tnnt *tenant.WithDetails) (job.Destination, error) {
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

	return job.DestinationFrom(destination.URN())
}

func (p JobPluginService) GenerateDependencies(ctx context.Context, jobSpec *dto.JobSpec, dryRun bool) ([]job.Source, error) {
	panic("implement")
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
