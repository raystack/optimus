package config

import (
	"fmt"
	"strconv"
)

type Optimus struct {
	// configuration version
	Version int `mapstructure:"version"`
	// optimus server host
	Host string `mapstructure:"host"`

	Project    Project      `mapstructure:"project"`
	Namespaces []*Namespace `mapstructure:"namespaces"`

	Server    Serve           `mapstructure:"serve"`
	Log       LogConfig       `mapstructure:"log"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Telemetry TelemetryConfig `mapstructure:"telemetry"`

	namespaceNameToNamespace map[string]*Namespace
}

func (o *Optimus) GetNamespaceByName(name string) (*Namespace, error) {
	if o.namespaceNameToNamespace == nil {
		o.namespaceNameToNamespace = make(map[string]*Namespace)
		for _, namespace := range o.Namespaces {
			o.namespaceNameToNamespace[namespace.Name] = namespace
		}
	}
	if o.namespaceNameToNamespace[name] == nil {
		return nil, fmt.Errorf("namespace [%s] is not found", name)
	}
	return o.namespaceNameToNamespace[name], nil
}

func (o *Optimus) GetVersion() string {
	return strconv.Itoa(o.Version)
}
