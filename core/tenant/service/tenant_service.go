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
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
	GetAll(ctx context.Context, projName tenant.ProjectName, namespaceName string) ([]*tenant.PlainTextSecret, error)
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

	namespace, err := t.namespaceGetter.Get(ctx, tnnt.ProjectName(), tnnt.NamespaceName())
	if err != nil {
		return nil, err
	}

	return tenant.NewTenantDetails(proj, namespace)
}

func (t TenantService) GetProject(ctx context.Context, name tenant.ProjectName) (*tenant.Project, error) {
	if name == "" {
		return nil, errors.InvalidArgument(tenant.EntityTenant, "invalid project name")
	}
	return t.projGetter.Get(ctx, name)
}

func (t TenantService) GetSecrets(ctx context.Context, projName tenant.ProjectName, nsName string) ([]*tenant.PlainTextSecret, error) {
	if projName == "" {
		return nil, errors.InvalidArgument(tenant.EntityTenant, "invalid project name")
	}
	return t.secretsGetter.GetAll(ctx, projName, nsName)
}

func (t TenantService) GetSecret(ctx context.Context, projName tenant.ProjectName, nsName string, name string) (*tenant.PlainTextSecret, error) {
	if projName == "" {
		return nil, errors.InvalidArgument(tenant.EntityTenant, "invalid project name")
	}
	return t.secretsGetter.Get(ctx, projName, nsName, name)
}

func NewTenantService(projGetter ProjectGetter, nsGetter NamespaceGetter, secretsGetter SecretsGetter) *TenantService {
	return &TenantService{
		projGetter:      projGetter,
		namespaceGetter: nsGetter,
		secretsGetter:   secretsGetter,
	}
}
