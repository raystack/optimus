package service

import (
	"context"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type NamespaceGetter interface {
	Get(context.Context, tenant.ProjectName, tenant.NamespaceName) (*tenant.Namespace, error)
}

type SecretsGetter interface {
	Get(ctx context.Context, ten tenant.Tenant, name string) (*tenant.PlainTextSecret, error)
	GetAll(ctx context.Context, ten tenant.Tenant) ([]*tenant.PlainTextSecret, error)
}

type TenantService struct {
	projGetter      ProjectGetter
	namespaceGetter NamespaceGetter
	secretsGetter   SecretsGetter
}

func (t TenantService) GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error) {
	if tnnt.ProjectName() == "" {
		return nil, errors.InvalidArgument(tenant.EntityTenant, "invalid tenant details provided")
	}

	proj, err := t.projGetter.Get(ctx, tnnt.ProjectName())
	if err != nil {
		return nil, err
	}

	var namespace *tenant.Namespace
	if nsName, err := tnnt.NamespaceName(); err == nil {
		namespace, err = t.namespaceGetter.Get(ctx, tnnt.ProjectName(), nsName)
		if err != nil {
			return nil, err
		}
	}

	return tenant.NewTenantDetails(proj, namespace)
}

func (t TenantService) GetSecrets(ctx context.Context, tnnt tenant.Tenant) ([]*tenant.PlainTextSecret, error) {
	return t.secretsGetter.GetAll(ctx, tnnt)
}

func (t TenantService) GetSecret(ctx context.Context, tnnt tenant.Tenant, name string) (*tenant.PlainTextSecret, error) {
	return t.secretsGetter.Get(ctx, tnnt, name)
}

func NewTenantService(projGetter ProjectGetter, nsGetter NamespaceGetter, secretsGetter SecretsGetter) *TenantService {
	return &TenantService{
		projGetter:      projGetter,
		namespaceGetter: nsGetter,
		secretsGetter:   secretsGetter,
	}
}
