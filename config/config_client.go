package config

import "fmt"

type ClientConfig struct {
	Version    Version      `mapstructure:"version"`
	Log        LogConfig    `mapstructure:"log"`
	Host       string       `mapstructure:"host"` // optimus server host
	Project    Project      `mapstructure:"project"`
	Namespaces []*Namespace `mapstructure:"namespaces"`

	namespaceNameToNamespace map[string]*Namespace
}

type Datastore struct {
	Type   string            `mapstructure:"type"`   // type could be bigquery/postgres/gcs
	Path   string            `mapstructure:"path"`   // directory to find specifications
	Backup map[string]string `mapstructure:"backup"` // backup configuration
}

type Job struct {
	Path string `mapstructure:"path"` // directory to find specifications
}

type Project struct {
	Name   string            `mapstructure:"name"`
	Config map[string]string `mapstructure:"config"`
}

type Namespace struct {
	Name      string            `mapstructure:"name"`
	Config    map[string]string `mapstructure:"config"`
	Job       Job               `mapstructure:"job"`
	Datastore []Datastore       `mapstructure:"datastore"`
}

func (c *ClientConfig) GetNamespaceByName(name string) (*Namespace, error) {
	if c.namespaceNameToNamespace == nil {
		c.namespaceNameToNamespace = map[string]*Namespace{}
		for _, namespace := range c.Namespaces {
			if namespace == nil {
				continue
			}
			c.namespaceNameToNamespace[namespace.Name] = namespace
		}
	}

	if c.namespaceNameToNamespace[name] == nil {
		return nil, fmt.Errorf("namespace [%s] is not found", name)
	}

	return c.namespaceNameToNamespace[name], nil
}
