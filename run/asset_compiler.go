package run

import (
	"context"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

type AssetCompiler interface {
	// Compile prepares instance execution context environment
	Compile(ctx context.Context, namespaceSpec models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (assets *models.JobRunInput, err error)
}

type compiler struct {
	secretService  service.SecretService
	templateEngine models.TemplateEngine
}

func (c *compiler) Compile(ctx context.Context, namespace models.NamespaceSpec, jobRun models.JobRun, instanceSpec models.InstanceSpec) (
	*models.JobRunInput, error) {
	secrets, err := c.secretService.GetSecrets(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return NewContextManager(namespace, secrets, jobRun, c.templateEngine).Generate(instanceSpec)
}

func NewAssetCompiler(secretService service.SecretService, te models.TemplateEngine) *compiler {
	return &compiler{
		secretService:  secretService,
		templateEngine: te,
	}
}
