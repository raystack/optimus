package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/models"
)

var (
	ErrDependencyModNotFound = errors.New("dependency mod not found for plugin")
)

type PluginService interface {
	GenerateDestination(context.Context, models.JobSpec, models.NamespaceSpec) (*models.GenerateDestinationResponse, error)
	GenerateDependencies(context.Context, models.JobSpec, models.NamespaceSpec, bool) (*models.GenerateDependenciesResponse, error)
}

type AssetCompiler func(ctx context.Context, jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error)

type DependencyPluginService struct {
	logger        log.Logger
	secretService SecretService
	pluginRepo    models.PluginRepository
	engine        models.TemplateEngine

	assetCompiler AssetCompiler
	Now           func() time.Time
}

func (d DependencyPluginService) GenerateDestination(ctx context.Context, jobSpec models.JobSpec, ns models.NamespaceSpec) (*models.GenerateDestinationResponse, error) {
	plugin, err := d.pluginRepo.GetByName(jobSpec.Task.Unit.Info().Name)
	if err != nil {
		return nil, err
	}

	if plugin.DependencyMod == nil {
		return nil, ErrDependencyModNotFound
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

func (d DependencyPluginService) GenerateDependencies(ctx context.Context, jobSpec models.JobSpec, ns models.NamespaceSpec, dryRun bool) (*models.GenerateDependenciesResponse, error) {
	var err error
	if jobSpec.Assets, err = d.assetCompiler(ctx, jobSpec, d.Now()); err != nil {
		return nil, fmt.Errorf("asset compilation failure: %w", err)
	}

	plugin, err := d.pluginRepo.GetByName(jobSpec.Task.Unit.Info().Name)
	if err != nil {
		return nil, err
	}

	if plugin.DependencyMod == nil {
		return nil, ErrDependencyModNotFound
	}

	compiledConfigs, err := d.compileConfig(ctx, jobSpec.Task.Config, ns)
	if err != nil {
		return nil, err
	}

	resp, err := plugin.DependencyMod.GenerateDependencies(ctx, models.GenerateDependenciesRequest{
		Config:  compiledConfigs,
		Assets:  models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
		Project: ns.ProjectSpec,
		PluginOptions: models.PluginOptions{
			DryRun: dryRun,
		},
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

	var confs models.PluginConfigs
	for _, config := range configs {
		compiledCnf, err := d.engine.CompileString(config.Value, tmplCtx)
		if err != nil {
			d.logger.Warn(" error in template compilation :: ", err.Error())
			compiledCnf = config.Value
		}
		confs = append(confs, models.PluginConfig{
			Name:  config.Name,
			Value: compiledCnf,
		})
	}
	return confs, nil
}

func NewPluginService(secretService SecretService, pluginRepo models.PluginRepository, engine models.TemplateEngine,
	logger log.Logger, assetCompiler AssetCompiler) *DependencyPluginService {
	return &DependencyPluginService{
		logger:        logger,
		secretService: secretService,
		pluginRepo:    pluginRepo,
		engine:        engine,
		assetCompiler: assetCompiler,
		Now:           time.Now,
	}
}
