package service

import (
	"context"

	"github.com/raystack/optimus/core/tenant"
)

type NamespaceRepository interface {
	Save(ctx context.Context, namespace *tenant.Namespace) error
	GetByName(context.Context, tenant.ProjectName, tenant.NamespaceName) (*tenant.Namespace, error)
	GetAll(context.Context, tenant.ProjectName) ([]*tenant.Namespace, error)
}

type NamespaceService struct {
	nsRepo NamespaceRepository
}

func (ns NamespaceService) Save(ctx context.Context, namespace *tenant.Namespace) error {
	return ns.nsRepo.Save(ctx, namespace)
}

func (ns NamespaceService) Get(ctx context.Context, projName tenant.ProjectName, namespaceName tenant.NamespaceName) (*tenant.Namespace, error) {
	return ns.nsRepo.GetByName(ctx, projName, namespaceName)
}

func (ns NamespaceService) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*tenant.Namespace, error) {
	return ns.nsRepo.GetAll(ctx, projectName)
}

func NewNamespaceService(nsRepo NamespaceRepository) *NamespaceService {
	return &NamespaceService{
		nsRepo: nsRepo,
	}
}
