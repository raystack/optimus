package config

import (
	"fmt"
	"strings"
)

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
		c.buildDictionary()
	}

	if c.namespaceNameToNamespace[name] == nil {
		return nil, fmt.Errorf("namespace [%s] is not found", name)
	}

	return c.namespaceNameToNamespace[name], nil
}

func (c *ClientConfig) ValidateNamespaceNames(namespaceNames ...string) error {
	if c.namespaceNameToNamespace == nil {
		c.buildDictionary()
	}

	var invalidNames []string
	for _, n := range namespaceNames {
		if c.namespaceNameToNamespace[n] == nil {
			invalidNames = append(invalidNames, n)
		}
	}
	var err error
	if len(invalidNames) > 0 {
		err = fmt.Errorf("namespace names [%s] are invalid", strings.Join(invalidNames, ", "))
	}
	return err
}

func (c *ClientConfig) GetSelectedNamespaces(namespaceNames ...string) ([]*Namespace, error) {
	if err := c.ValidateNamespaceNames(namespaceNames...); err != nil {
		return nil, err
	}
	output := make([]*Namespace, len(namespaceNames))
	for i, n := range namespaceNames {
		output[i] = c.namespaceNameToNamespace[n]
	}
	return output, nil
}

func (c *ClientConfig) GetAllNamespaceNames() []string {
	output := make([]string, len(c.Namespaces))
	for i, n := range c.Namespaces {
		output[i] = n.Name
	}
	return output
}

func (c *ClientConfig) buildDictionary() {
	c.namespaceNameToNamespace = map[string]*Namespace{}
	for _, namespace := range c.Namespaces {
		if namespace == nil {
			continue
		}
		c.namespaceNameToNamespace[namespace.Name] = namespace
	}
}
