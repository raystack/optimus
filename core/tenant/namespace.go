package tenant

import (
	"github.com/raystack/optimus/internal/errors"
)

const EntityNamespace = "namespace"

type NamespaceName string

func NamespaceNameFrom(name string) (NamespaceName, error) {
	if name == "" {
		return "", errors.InvalidArgument(EntityNamespace, "namespace name is empty")
	}

	return NamespaceName(name), nil
}

func (n NamespaceName) String() string {
	return string(n)
}

type Namespace struct {
	name NamespaceName

	projectName ProjectName
	config      map[string]string
}

func (n *Namespace) Name() NamespaceName {
	return n.name
}

func (n *Namespace) ProjectName() ProjectName {
	return n.projectName
}

func (n *Namespace) GetConfig(key string) (string, error) {
	for k, v := range n.config {
		if key == k {
			return v, nil
		}
	}
	return "", errors.NotFound(EntityNamespace, "namespace config not found "+key)
}

// GetConfigs returns a clone on project configurations
func (n *Namespace) GetConfigs() map[string]string {
	confs := make(map[string]string, len(n.config))
	for k, v := range n.config {
		confs[k] = v
	}
	return confs
}

func NewNamespace(name string, projName ProjectName, config map[string]string) (*Namespace, error) {
	nsName, err := NamespaceNameFrom(name)
	if err != nil {
		return nil, err
	}

	if projName == "" {
		return nil, errors.InvalidArgument(EntityNamespace, "project name is empty")
	}

	return &Namespace{
		name:        nsName,
		config:      config,
		projectName: projName,
	}, nil
}
