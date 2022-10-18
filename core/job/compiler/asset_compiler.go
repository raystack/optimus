package compiler

import (
	"github.com/odpf/optimus/core/job/dto"
	"github.com/odpf/optimus/models"
	"golang.org/x/net/context"
	"time"
)

type AssetCompiler struct {
	engine     models.TemplateEngine
	pluginRepo models.PluginRepository
}

func NewAssetCompiler(engine models.TemplateEngine, pluginRepo models.PluginRepository) *AssetCompiler {
	return &AssetCompiler{engine: engine, pluginRepo: pluginRepo}
}

func (c *AssetCompiler) CompileAssets(ctx context.Context, jobSpec *dto.JobSpec, scheduledAt time.Time, contextForTask map[string]interface{}) (map[string]string, error) {
	panic("implement")
}
