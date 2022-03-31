package service

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/models"
)

type PluginService interface {
	GenerateDestination(context.Context, models.JobSpec, models.NamespaceSpec) (*models.GenerateDestinationResponse, error)
	GenerateDependencies(context.Context, models.JobSpec, models.NamespaceSpec) (*models.GenerateDependenciesResponse, error)
}

type DependencyPluginService struct {
	secretService SecretService
	pluginRepo    models.PluginRepository
	engine        models.TemplateEngine
}

func (d DependencyPluginService) GenerateDestination(ctx context.Context, jobSpec models.JobSpec, ns models.NamespaceSpec) (*models.GenerateDestinationResponse, error) {
	plugin, err := d.pluginRepo.GetByName(jobSpec.Task.Unit.Info().Name)
	if err != nil {
		return nil, err
	}

	if plugin.DependencyMod == nil {
		return nil, nil //nolint:nilnil // TODO: decide based on caller to return error or nil
	}

	compiledConfigs, err := d.compileConfig(ctx, jobSpec.Task.Config, ns)
	if err != nil {
		return nil, err
	}

	destinationResp, err := plugin.DependencyMod.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config:  compiledConfigs,
		Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
		Project: ns.ProjectSpec,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate destination: %w", err)
	}

	return destinationResp, nil
}

func (d DependencyPluginService) GenerateDependencies(ctx context.Context, jobSpec models.JobSpec, ns models.NamespaceSpec) (*models.GenerateDependenciesResponse, error) {
	plugin, err := d.pluginRepo.GetByName(jobSpec.Task.Unit.Info().Name)
	if err != nil {
		return nil, err
	}

	if plugin.DependencyMod == nil {
		return nil, nil //nolint:nilnil // TODO: decide based on caller to return error or nil
	}

	compiledConfigs, err := d.compileConfig(ctx, jobSpec.Task.Config, ns)
	if err != nil {
		return nil, err
	}

	resp, err := plugin.DependencyMod.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
		Config:  compiledConfigs,
		Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
		Project: ns.ProjectSpec,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (d DependencyPluginService) compileConfig(ctx context.Context, configs models.JobSpecConfigs, namespace models.NamespaceSpec) (models.PluginConfigs, error) {
	var secrets models.ProjectSecrets
	var err error
	if secrets, err = d.secretService.GetSecrets(ctx, namespace); err != nil {
		return nil, err
	}

	tmplCtx := compiler.PrepareContext(
		compiler.From(namespace.ProjectSpec.Config, namespace.Config).WithName("proj").WithKeyPrefix(compiler.ProjectConfigPrefix),
		compiler.From(secrets.ToMap()).WithName("secret"),
	)

	for i, config := range configs {
		compiledCnf, err := d.engine.CompileString(config.Value, tmplCtx)
		if err == nil {
			configs[i].Value = compiledCnf
		}
	}
	return models.PluginConfigs{}.FromJobSpec(configs), nil
}

func NewPluginService(secretService SecretService, pluginRepo models.PluginRepository, engine models.TemplateEngine) *DependencyPluginService {
	return &DependencyPluginService{
		secretService: secretService,
		pluginRepo:    pluginRepo,
		engine:        engine,
	}
}
