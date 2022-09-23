package tenant

import (
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/utils"
)

const EntityTenant = "tenant"

// Tenant should use ProjectName and NamespaceName as members
type Tenant struct {
	projName ProjectName
	nsName   *NamespaceName
}

func (t Tenant) ProjectName() ProjectName {
	return t.projName
}

func (t Tenant) NamespaceName() (NamespaceName, error) {
	if t.nsName == nil {
		return "", errors.NotFound(EntityTenant, "namespace name is not present")
	}
	return *t.nsName, nil
}

func NewTenant(projectName string, namespaceName string) (Tenant, error) {
	projName, err := ProjectNameFrom(projectName)
	if err != nil {
		return Tenant{}, err
	}

	if namespaceName == "" {
		return Tenant{
			projName: projName,
		}, nil
	}

	nsName, _ := NamespaceNameFrom(namespaceName)
	return Tenant{
		projName: projName,
		nsName:   &nsName,
	}, nil
}

type WithDetails struct {
	project   Project
	namespace *Namespace
}

func NewTenantDetails(proj *Project, namespace *Namespace) (*WithDetails, error) {
	if proj == nil {
		return nil, errors.InvalidArgument(EntityTenant, "project is nil")
	}

	return &WithDetails{
		project:   *proj,
		namespace: namespace,
	}, nil
}

func (w *WithDetails) ToTenant() Tenant {
	projName := w.project.Name()

	if w.namespace == nil {
		return Tenant{
			projName: projName,
		}
	}

	nsName := w.namespace.Name()
	return Tenant{
		projName: projName,
		nsName:   &nsName,
	}
}

func (w *WithDetails) GetConfig(key string) (string, error) {
	if w.namespace != nil {
		config, err := w.namespace.GetConfig(key)
		if err == nil {
			return config, nil
		}
	}

	// key not present in namespace, check project
	config, err := w.project.GetConfig(key)
	if err == nil {
		return config, nil
	}

	return "", errors.NotFound(EntityTenant, "config not present in tenant "+key)
}

func (w *WithDetails) GetConfigs() map[string]string {
	var m1 map[string]string
	if w.namespace != nil {
		m1 = w.namespace.GetConfigs()
	}

	return utils.MergeMaps(w.project.GetConfigs(), m1)
}

func (w *WithDetails) Project() *Project {
	return &w.project
}

func (w *WithDetails) Namespace() (*Namespace, error) {
	if w.namespace == nil {
		return nil, errors.NotFound(EntityTenant, "namespace is not present")
	}

	return w.namespace, nil
}
