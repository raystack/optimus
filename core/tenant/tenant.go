package tenant

import (
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/utils"
)

const EntityTenant = "tenant"

type Tenant struct {
	projName ProjectName
	nsName   NamespaceName
}

func (t Tenant) ProjectName() ProjectName {
	return t.projName
}

func (t Tenant) NamespaceName() NamespaceName {
	return t.nsName
}

func (t Tenant) IsInvalid() bool {
	return t.projName.String() == ""
}

func NewTenant(projectName, namespaceName string) (Tenant, error) {
	projName, err := ProjectNameFrom(projectName)
	if err != nil {
		return Tenant{}, err
	}

	nsName, err := NamespaceNameFrom(namespaceName)
	if err != nil {
		return Tenant{}, err
	}

	return Tenant{
		projName: projName,
		nsName:   nsName,
	}, nil
}

type WithDetails struct {
	project    Project
	namespace  Namespace
	secretsMap map[string]string
}

func NewTenantDetails(proj *Project, namespace *Namespace, secrets PlainTextSecrets) (*WithDetails, error) {
	if proj == nil {
		return nil, errors.InvalidArgument(EntityTenant, "project is nil")
	}
	if namespace == nil {
		return nil, errors.InvalidArgument(EntityTenant, "namespace is nil")
	}

	return &WithDetails{
		project:    *proj,
		namespace:  *namespace,
		secretsMap: secrets.ToMap(),
	}, nil
}

func (w *WithDetails) ToTenant() Tenant {
	return Tenant{
		projName: w.project.Name(),
		nsName:   w.namespace.Name(),
	}
}

func (w *WithDetails) GetConfig(key string) (string, error) {
	config, err := w.namespace.GetConfig(key)
	if err == nil {
		return config, nil
	}

	// key not present in namespace, check project
	config, err = w.project.GetConfig(key)
	if err == nil {
		return config, nil
	}

	return "", errors.NotFound(EntityTenant, "config not present in tenant "+key)
}

func (w *WithDetails) GetConfigs() map[string]string {
	m1 := w.namespace.GetConfigs()
	return utils.MergeMaps(w.project.GetConfigs(), m1)
}

func (w *WithDetails) Project() *Project {
	return &w.project
}

func (w *WithDetails) Namespace() *Namespace {
	return &w.namespace
}

func (w *WithDetails) SecretsMap() map[string]string {
	return w.secretsMap
}
